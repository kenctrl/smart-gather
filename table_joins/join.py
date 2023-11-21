import csv
import time
import pandas as pd

import sys
sys.path.append("../")
from file_processing.utils.glove_col_similarity import * 
sys.path.pop()

def join_tables(desired_headers, intersection, result_filename):
    er_info = {}
    for file1, file2 in intersection:
        for filename in [file1, file2]:
                df = pd.read_csv(filename)
                columns = df.to_dict(orient='list')
                for cname in columns:
                    if cname in desired_headers:
                        er_info[cname] = columns[cname]

    with open(result_filename, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(er_info.keys())
        csv_writer.writerows(zip(*er_info.values()))

    print("er columns:", er_info)

def get_headers(filenames):
    headers_per_file = {}
    for filename in filenames:
        with open(filename, 'r') as file:
            csv_reader = csv.reader(file, delimiter="\n")
            header = next(csv_reader)[0].split(',')
            header = [col.replace('\ufeff', '') for col in header]
            headers_per_file[filename] = header

    return headers_per_file

def get_best_intersection(cols1, cols2, embedding_space):
    """
    Given two sets of column headers, determine which pair of column headers are the most similar

    Return the two columns and their similarity score
    """

    best_col1, best_col2, highest_similarity = None, None, -float('inf')

    for c1 in cols1:
        c1_embedding = get_phrase_embedding(c1, embedding_space)
        for c2 in cols2:
            c2_embedding = get_phrase_embedding(c2, embedding_space)

            if c1 == c2: # found perfect match, return early
                print("column names match, returning early:", c1, c2)
                return c1, c2, 1.0
            
            if c1_embedding is not None and c2_embedding is not None:
                similarity = 1 - cosine(c1_embedding, c2_embedding)
                if similarity > highest_similarity:
                    best_col1, best_col2, highest_similarity = c1, c2, similarity

    return best_col1, best_col2, highest_similarity
                

def find_header_intersection(csv_headers, embedding_space):
    """
    Given the set of files + column headers that we need to join across, find the column most similar 
    across each pair that will be the target of the join

    Return format: dict mapping filenames part of the intersection to cols that most resemble each other
    (file1, file2) -> (best col name match in file1, best col name match in file2)
    """

    filenames = [fn for fn in csv_headers.keys()]
    best_intersection = {}
    
    for i in range(len(filenames)):
        cols1 = csv_headers[filenames[i]]
        for j in range(i+1, len(filenames)):
            cols2 = csv_headers[filenames[j]]
            best_intersection[(filenames[i], filenames[j])] = get_best_intersection(cols1, cols2, embedding_space)

    return best_intersection
        

def get_matches(schema_headers, csv_headers):
    """
    Given the schema headers and a mapping of each file to its column headers,
    determine per schema column header match to the best csv header match across all files

    Group two ways:
    1. dict mapping schema header -> (file best match is found in, name of best column match, similarity score)
    2. dict mapping filename -> list of tuples, where each tuple contains (best column match name, schema column name)
    """

    matches = {} # per col, then per file
    for schema_header in schema_headers:
        matches[schema_header] = {}
        for filename, headers in csv_headers.items():
            matches[schema_header][filename] = get_schema_header_match(schema_header, headers, embedding_space)
    
    cols_to_matches = {} # map schema header to (file containing best match, match col name, similarity score)
    for schema_header, match_info in matches.items():
        best_file, closest_col_name, highest_sim_score = None, None, -float('inf')
        for filename, scores in match_info.items():
            if len(scores) == 0:
                continue
            col_name, similarity_score = scores[0] # already sorted in decreasing similarity order
            if highest_sim_score < similarity_score:
                best_file, closest_col_name, highest_sim_score = filename, col_name, similarity_score
        
        cols_to_matches[schema_header] = (best_file, closest_col_name, highest_sim_score)

    files_to_matches = {} # map each file to the set of columns that it is providing information for (map its column name to the schema column that its matching)
    for schema_col, match_info in cols_to_matches.items():
        filename, col, score = match_info
        files_to_matches[filename] = files_to_matches.get(filename, []) + [(col, schema_col)]

    return cols_to_matches, files_to_matches

if __name__ == '__main__':
    embedding_space = get_glove_embedding_space()
    schema_headers = ["rain", "temperature"]
    csv_headers = {
        "file1:": ["weather", "georgia", "plaid", "tulips"],
        "file2": ["precipitation", "celsius", "fahrenheit", "roses"]
    }
    cols_to_matches, files_to_matches = get_matches(schema_headers, csv_headers)

    print("cols to matches:", cols_to_matches)
    print()
    print("files to matches:", files_to_matches)
    print()

    if len(files_to_matches) > 1: # means that best columns are in different files, must execute a join
        intersection = find_header_intersection(csv_headers, embedding_space) # assume just 2 files for now
        print("intersection:", intersection)
