import openai
from manual_join import get_headers, get_matches
from gpt_optimizations.gpt_column_headers import get_data_sample

OPENAI_API_KEY = "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"
client = openai.OpenAI(api_key = OPENAI_API_KEY)

def get_gpt_input(schema_headers, csv_headers, filename_1, filename_2):
    """
    Given a set of csv headers, return the set of headers that are common across all csv files
    """
    gpt_input = ""
    for idx, (file_name, headers) in enumerate([(filename_1, csv_headers[filename_1]), (filename_2, csv_headers[filename_2])]):
        headers = [header for header in headers if header != ''] # remove empty string headers
        header, data = get_data_sample(file_name, random_sample=True)
        # Get schema headers ", ".join(headers)
        gpt_input += f"Schema {idx}:\n" + header + "\n\n"
        # Get sample rows from file name
        gpt_input += f"Sample rows from Schema {idx}:\n"
        gpt_input += data + "\n\n"
    gpt_input += "Final schema headers:\n" + ", ".join(schema_headers) + "\n\n"

    return gpt_input

def find_header_intersection_gpt(schema_headers, csv_headers, files_to_matches, print_results=False):
    """
    Given the set of files + column headers that we need to join across, find the column most similar
    across each pair that will be the target of the join

    Return format: dict mapping filenames part of the intersection to cols that most resemble each other
    (file1, file2) -> (best col name match in file1, best col name match in file2)
    """
    filenames = [fn for fn in files_to_matches.keys()]
    intersections = []

    for i in range(len(filenames)):
        cols1 = csv_headers[filenames[i]]
        for j in range(i+1, len(filenames)):
            cols2 = csv_headers[filenames[j]]

            gpt_input = get_gpt_input(schema_headers, csv_headers, filenames[i], filenames[j])
            prompt = "Match each column from Schema 0 to a column from Schema 1 only if they represent the " + \
                    "same data. Output the answer only as a list of length-two tuples that contain the corresponding " + \
                    "columns from Schema 0 and Schema 1, with no other output.\n"
            # prompt = "Match each column from each schema to a column from each other schema only if they represent the " + \
            #         "same data. Output the answer only as a list of length-three tuples that contain the corresponding " + \
            #         "columns from Schema 0 and Schema 1 as the first two values, and a comma-separated string representing " + \
            #         "which two schemas the tuple represents, with no other output. For example, a join on Schema 0 and Schema 1 " + \
            #         "whould have tuples of the form ('column from schema 0', 'column from schema 1', '0,1').\n\n"
            if print_results:
                print("gpt input:", gpt_input)
                print()
                print("gpt prompt:", prompt)
                print()

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                {"role": "system",
                "content": gpt_input + prompt}
                ],
                temperature=0,
                max_tokens=1024
            )
            if print_results:
                print("gpt output:", response.choices[0].message.content)

            str_response = response.choices[0].message.content

            # Convert string into list of tuples
            str_response = str_response.replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("'", "").replace('"', "").replace("\n", "")
            str_response = str_response.split(", ")
            lis_response = []
            for s in range(0, len(str_response)-1, len(files_to_matches)):
                schema_0_col = str_response[s]
                schema_1_col = str_response[s+1]
                # Find col in schema 0 and schema 1 that matches the col name (agnostic of spaces)
                for col in cols1:
                    if col.replace(" ", "") == schema_0_col.replace(" ", ""):
                        schema_0_col = col
                        break
                for col in cols2:
                    if col.replace(" ", "") == schema_1_col.replace(" ", ""):
                        schema_1_col = col
                        break
                lis_response.append((schema_0_col, schema_1_col))
            if print_results:
                print("response:", lis_response)

            best_intersections = [(1, c1, c2) for c1, c2 in lis_response]
            new_intersections = [(sim, filenames[i], filenames[j], col1, col2) for sim, col1, col2 in best_intersections]

            intersections.extend(new_intersections)

    intersections.sort(reverse=True)

    # move intersections from list format sorted by similarity to keyed by the file pair
    # allows us to get multiple cols to potentially join on given a file pair
    file_based_intersections = {}

    for sim, f1, f2, c1, c2 in intersections:
        if (f1, f2) not in file_based_intersections:
            file_based_intersections[(f1, f2)] = []
        file_based_intersections[(f1, f2)].append((c1, c2, sim))

    # used to make sure we don't do unnecessary joins between files
    # ex: files A, B, C.  if we join A with C and B with C, no need to rejoin A + B
    seen_cols = set()
    final_intersections = {}

    for similarity_score, f1, f2, col1, col2 in intersections:
        if len(seen_cols) == len(files_to_matches):
            break
        if f1 not in seen_cols or f2 not in seen_cols:
            seen_cols.add(f1)
            seen_cols.add(f2)
            final_intersections[(f1, f2)] = file_based_intersections[(f1, f2)]

    return final_intersections

def plan_join(files, schema_headers, verbose=False):
    """
    Given the set of files and the schema headers, plan the join by finding the best column match
    for each schema header across all files

    Return format: dict mapping schema header -> (file best match is found in, name of best column match, similarity score)
    """
    csv_headers = { file: get_headers(file) for file in files }
    gpt_input = ""
    for idx, headers in enumerate(csv_headers.values()):
        # Remove empty string headers
        headers = [header for header in headers if header != '']
        gpt_input += f"Schema {idx}:\n" + ", ".join(headers) + "\n\n"
    cols_to_matches, files_to_matches = get_matches(schema_headers, csv_headers)

    if verbose:
        print("cols to matches:", cols_to_matches)
        print()
        print("files to matches:", files_to_matches)
        print()

    plan = {
        'cols_to_matches': cols_to_matches,
        'files_to_matches': files_to_matches,
        'intersections': None
    }

    # csv_headers = { file: get_headers(file) for file in files }
    # gpt_input = get_gpt_input(schema_headers, csv_headers)
    # if verbose:
    #     print("gpt input:", gpt_input)
    #     print()

    # prompt = "Given Schema 0 and Schema 1, sample rows for each schema, and the final schema headers, generate a mapping from each final schema header to the one best column match in Schema 0 or Schema 1. The output format is a list of length-three tuples that contain the corresponding final schema header, the schema it came from, and the column name from that schema that it matches to.\n"
    # # match each column from Schema 0 to a column from Schema 2 only if they represent the same data. Output the answer only as a list of length-three tuples that contain the corresponding columns from Schema 0 and Schema 1 as the first two elements and a name representing both columns as the third element, with no other output."
    # if print_results:
    #   print("gpt prompt:", prompt)
    #   print()

    # client = openai.OpenAI(api_key = OPENAI_API_KEY)
    # response = client.chat.completions.create(
    #   model="gpt-4",
    #   messages=[
    #   {"role": "system",
    #   "content": gpt_input + prompt}
    #   ],
    #   temperature=0,
    #   max_tokens=256
    # )

    # print_results = True
    # if print_results:
    #     print("gpt output:\n", response.choices[0].message.content)
    #     print()

    # str_response = response.choices[0].message.content

    # # Convert string into list of tuples
    # str_response = str_response.replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("'", "").replace('"', "").replace("\n", "")
    # str_response = str_response.split(", ")
    # lis_response = []
    # for i in range(0, len(str_response), 3):
    #     lis_response.append((str_response[i], str_response[i+1], str_response[i+2]))
    # print("response:", lis_response)

    # cols_to_matches = {}
    # for col, schema_name, schema_col_name in lis_response:
    #     schema_idx = int(schema_name.split(" ")[1])
    #     cols_to_matches[col] = (files[schema_idx], schema_col_name, 1)

    # files_to_matches = {}
    # for schema_col, match_info in cols_to_matches.items():
    #     filename, col, score = match_info
    #     files_to_matches[filename] = files_to_matches.get(filename, []) + [(col, schema_col)]

    # print("cols to matches:", cols_to_matches)
    # print()
    # print("files to matches:", files_to_matches)
    # print()

    # plan = {
    #     'cols_to_matches': cols_to_matches,
    #     'files_to_matches': files_to_matches,
    #     'intersections': None
    # }

    if len(files_to_matches) > 1:
        plan['intersections'] = find_header_intersection_gpt(schema_headers, csv_headers, files_to_matches, print_results=verbose)

        if verbose:
            print("intersections:", plan['intersections'])
            print()

    return plan
