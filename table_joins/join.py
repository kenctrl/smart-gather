import csv
import time
import pandas as pd

from multi_table_join import MultiTableJoin

import sys
sys.path.append("../")
from file_processing.utils.glove_col_similarity import *
sys.path.pop()

def get_headers(filenames):
    headers_per_file = {}
    for filename in filenames:
        with open(filename, 'r') as file:
            csv_reader = csv.reader(file, delimiter="\n")
            header = next(csv_reader)[0].split(',')
            header = [col.replace('\ufeff', '') for col in header]
            headers_per_file[filename] = header

    return headers_per_file


def join_tables(files_to_matches, intersection, schema_headers, result_filename):
    """
    Given the mapping of files to the columns that they are providing information for,
    and the intersection of the two files, join the two tables together
    """

    join = MultiTableJoin(intersection, schema_headers, files_to_matches)
    print(join)
    result = join.get_result(result_filename)
    if result is None:
        print("Could not join tables")


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


def find_header_intersection(csv_headers, embedding_space, num_files):
    """
    Given the set of files + column headers that we need to join across, find the column most similar
    across each pair that will be the target of the join

    Return format: dict mapping filenames part of the intersection to cols that most resemble each other
    (file1, file2) -> (best col name match in file1, best col name match in file2)
    """

    filenames = [fn for fn in csv_headers.keys()]
    intersections = []

    for i in range(len(filenames)):
        cols1 = csv_headers[filenames[i]]
        for j in range(i+1, len(filenames)):
            cols2 = csv_headers[filenames[j]]
            col1, col2, similarity_score = get_best_intersection(cols1, cols2, embedding_space)
            intersections.append([similarity_score, filenames[i], filenames[j], col1, col2])

    intersections.sort(reverse=True)
    seen_cols = set()
    final_intersections = {}

    for similarity_score, f1, f2, col1, col2 in intersections:
        if len(seen_cols) == num_files:
            break
        if f1 not in seen_cols or f2 not in seen_cols:
            seen_cols.add(f1)
            seen_cols.add(f2)
            final_intersections[(f1, f2)] = tuple([col1, col2, similarity_score])

    return final_intersections


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
    files = ["./boys_to_girls.csv", "./women_in_parliment.csv"]
    headers = get_headers(files)

    embedding_space = get_glove_embedding_space()
    schema_headers = ["year", "country", "ratio"]
    csv_headers = { file: headers[file] for file in files }
    cols_to_matches, files_to_matches = get_matches(schema_headers, csv_headers)

    print("cols to matches:", cols_to_matches)
    print()
    print("files to matches:", files_to_matches)
    print()

    if len(files_to_matches) > 1: # means that best columns are in different files, must execute a join
        intersection = find_header_intersection(csv_headers, embedding_space, len(files))
        print("intersection:", intersection)
        print()

        join_tables(files_to_matches, intersection, schema_headers, "UN_dataset_join.csv")
