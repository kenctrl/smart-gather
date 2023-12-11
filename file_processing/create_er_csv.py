import csv
import requests
import time
import subprocess

from file_processing.utils.glove_col_similarity import *

def get_headers(scrape_result_file, timeout=5):
    """
    Given a list of CSVs from scraping, extract and yield headers from each file
    Restrict to a default 5-second timeout limit to prevent hanging

    Parameters:
    - scrape_result_file (str): File containing newline-separated URLs,
        each enabling CSV file download

    Yields:
    - url (str): URL enabling CSV file download
    - headers (list): the column headers within the CSV file at 'url'
    """

    with open(scrape_result_file, 'r', encoding='utf-8-sig') as f:
        header_info = []

        for scraped_url in f:
            url = scraped_url.strip()
            content = None

            try:
                result = subprocess.run(["curl", url], capture_output=True, text=True, timeout=timeout)
                if result.returncode == 0:
                    content = result.stdout

            except Exception as e:
                print(f"encountered error when getting from {url}:\n\t{e}")
                continue

            if content is None:
                print(f"unable to get data at {url}")
                continue

            csv_data = list(csv.reader(content.splitlines()))
            if len(csv_data) == 0:
                print(f"unable to get data at {url}")
                continue
            headers = csv_data[0]

            yield url, headers

def get_best_dataset(schema_headers, scrape_result_file):
    """
    Given a list of scraped files, determine each's similarity relative to schema
    headers. Per-CSV score calculated by finding avg similarity across schema headers.

    Parameters:
    - schema_headers (dict): Contains the following keys:
        - default_pk (list): Keys ending in _id that generated by schema creator
        - non_default_pk (list): All column headers that are't generated by schema creator
    - scrape_result_file (str): File containing newline-separated URLs,
        each enabling CSV file download

    Returns:
    - best_url (str): URL of the best match CSV file
    - best_score (float): similarity score between 0-1 of the best match file
    - all_metrics (dict): Contains the following:
        - file URL (string): URL of CSV file we're providing information for.
            - score (float): Similarity score across all column headers for this given file
            - column_mapping (dict): Has the following structure:
                - schema_header_name (str): Name of the desired schema header, maps to a tuple containing:
                    - First element (str): Name of CSV column that's most similar to 'schema_header_name'
                    - Second element (float): Number between [-1, 1] representing the cosine similarity
                        between this column name and the 'schema_header_name'
    """

    embedding_space = get_glove_embedding_space()

    all_metrics = {}
    best_url, best_score = None, -float('inf')

    for url, csv_headers in get_headers(scrape_result_file):
        print(f"\nprocessing {url}")
        csv_matches = get_csv_matches(schema_headers['non_default_pk'], csv_headers, embedding_space)
        score = sum([similarity for phrase, similarity in csv_matches.values()]) / len(csv_matches)

        if score > best_score:
            best_url, best_score = url, score

        all_metrics[url] = {
            "score": score,
            "column_mapping": csv_matches
        }

    return best_url, best_score, all_metrics

def create_er_csv(output_dir, schema_headers, best_url, column_mapping, max_rows=50):
    """
    Given the schema's column headers, the file containing the best match, and a mapping
    of schema header name to its best match in the provided file, generate a new CSV file
    with the best-match data.

    Parameters:
    - output_dir (str): Directory to write the output CSV file to
    - schema_headers (dict): Contains the following keys:
        - default_pk (list): Keys ending in _id that generated by schema creator
        - non_default_pk (list): All column headers that are't generated by schema creator
    - best_url (str): Contains the URL containing the best file match
    - column_mapping (dict): Has the following structure:
        - schema_header_name (str): Name of the desired schema header
            - First element (str): Name of CSV column that's most similar to 'schema_header_name'
            - Second element (float): Number between [-1, 1] representing the cosine similarity
                between this column name and the 'schema_header_name'
    - max_rows: Number of rows of data to write to the CSV we're generating
    """

    output_filename = f"schema_{time.time()}.csv"

    res = requests.get(best_url.strip())
    if res.status_code != 200:
        print(f"unable to get best file url {best_url}")

    content = res.text
    csv_data = list(csv.reader(content.splitlines()))
    file_headers = csv_data[0]

    # get the value in the CSV row corresponding to schema column header
    get_val = lambda row, schema_col: row[file_headers.index(column_mapping[schema_col][0])]

    er_rows = []
    print("column mapping:", column_mapping)
    end_ix = min(len(csv_data), max_rows+1)
    for row in csv_data[1:end_ix]: # add non-default PKs
        er_row = {schema_col: get_val(row, schema_col) for schema_col in schema_headers['non_default_pk']}
        er_rows.append(er_row)

    if len(schema_headers['default_pk']) != 0:
        pk = schema_headers['default_pk'][0] # add default PK
        for pk_id, row_info in enumerate(er_rows):
            row_info[pk] = pk_id
        for pk in schema_headers['default_pk'][1:]:
            for row_info in er_rows:
                row_info[pk] = "NULL"

    # Write the selected rows to the output CSV file
    with open(output_dir + "/" + output_filename, "w", newline="") as output_file:
        table_headers = schema_headers['default_pk'] + schema_headers['non_default_pk']
        writer = csv.DictWriter(output_file, fieldnames=table_headers)
        writer.writeheader()
        writer.writerows(er_rows)

    print(f"\nCSV file with requested headers has been created at {output_dir}/{output_filename}.")

def generate_smart_data(output_dir, schema_headers, scraped_url_file):
    """
    Given column headers and a file containing links to potential CSV file matches,
    find the best match and save to a fresh file the best-match data.

    Parameters:
    - output_dir (str): Directory to write the output CSV file to
    - schema_headers (dict): Contains the following keys:
        - default_pk (list): keys ending in _id that generated by schema creator
        - non_default_pk (list): all column headers that are't generated by schema creator
    - scraped_url_file (string): filename whose contents contain URLs of potential CSVs
    """

    best_url, best_score, all_metrics = get_best_dataset(schema_headers, scraped_url_file)
    ranked_urls = sorted(all_metrics.keys(), key=lambda url: -all_metrics[url]['score'])

    print()
    print(f"best url match: {best_url} (score: {best_score})")
    print()

    for url in ranked_urls:
        print("URL:", url)
        print("average score:", all_metrics[url]['score'])
        print(all_metrics[url]['column_mapping'])
        print()

    create_er_csv(output_dir, schema_headers, best_url, all_metrics[best_url]['column_mapping']) # TODO: match types

if __name__ == '__main__':
    scraped_url_file = "scrape_result_subset.txt"

    # month, day in all headers, sin in incoming_solar_final, ppt in precipitation_final
    # schema_headers = {'non_default_pk': ['month', 'day', 'sme2_sin_w/m2', 'sme2_ppt_mm']}
    # schema_headers = {
    #     'non_default_pk': ['water_year', 'calendar_year', 'sme2_rh']
    # }

    # generate_smart_data(output_dir, schema_headers, scrape_results)


