import csv
import time
import pandas as pd

from multi_table_join import MultiTableJoin

import sys
sys.path.append("../")
from file_processing.utils.glove_col_similarity import *
sys.path.pop()

embedding_space = get_glove_embedding_space()


def get_headers(filename):
    sniffer = csv.Sniffer()
    with open(filename, mode='r') as f:
        dialect = sniffer.sniff(f.read(1024))
        f.seek(0)
        csv_reader = csv.reader(f, delimiter=dialect.delimiter)
        a = next(csv_reader)
        f.close()
        return a
        # header = next(csv_reader)[0].split(';')
        # return header


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


def get_best_intersections(cols1, cols2, embedding_space):
    """
    Given two sets of column headers, determine which pair of column headers are the most similar

    Return the two columns and their similarity score
    """

    best_col1, best_col2, highest_similarity = None, None, -float('inf')

    # if multiple column headers are exact matches, return all of them
    # if none exactly match, return the single most similar match
    pairwise_similarities = []

    for c1 in cols1: # find pairwise similarity between each column between two files
        c1_embedding = get_phrase_embedding(c1, embedding_space)
        for c2 in cols2:
            if c1 == '' or c2 == '': # csv file allows for empty header
                continue
            c2_embedding = get_phrase_embedding(c2, embedding_space)

            if c1 == c2: # found perfect match, return early
                pairwise_similarities.append((1.0, c1, c2))

            elif c1_embedding is not None and c2_embedding is not None:
                similarity = 1 - cosine(c1_embedding, c2_embedding)
                pairwise_similarities.append((similarity, c1, c2))

    pairwise_similarities.sort(reverse=True)

    filtered_similarities = [] # get all pairs that exactly match + 1 non-exact match if it exists

    for i in range(len(pairwise_similarities)):
        filtered_similarities.append(pairwise_similarities[i])
        if pairwise_similarities[i][0] < 0.9: # only get first non-perfect join
            break

    return filtered_similarities


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
            best_intersections = get_best_intersections(cols1, cols2, embedding_space)
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
        if len(seen_cols) == num_files:
            break
        if f1 not in seen_cols or f2 not in seen_cols:
            seen_cols.add(f1)
            seen_cols.add(f2)
            final_intersections[(f1, f2)] = file_based_intersections[(f1, f2)]

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


def plan_join(files, schema_headers, print_results=False):
    """
    Given the set of files and the schema headers, plan the join by finding the best column match
    for each schema header across all files

    Return format: dict mapping schema header -> (file best match is found in, name of best column match, similarity score)
    """

    csv_headers = { file: get_headers(file) for file in files }
    cols_to_matches, files_to_matches = get_matches(schema_headers, csv_headers)

    if print_results:
        print("cols to matches:", cols_to_matches)
        print()
        print("files to matches:", files_to_matches)
        print()

    plan = {
        'cols_to_matches': cols_to_matches,
        'files_to_matches': files_to_matches,
        'intersections': None
    }

    if len(files_to_matches) > 1:
        subset = {f: csv_headers[f] for f in files_to_matches} # only find intersection for files that contain schema cols
        plan['intersections'] = find_header_intersection(subset, embedding_space, len(files_to_matches))

        if print_results:
            print("intersections:", plan['intersections'])
            print()

    return plan
