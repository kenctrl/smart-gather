import numpy as np
from collections import defaultdict
from scipy.spatial.distance import cosine
import re

words_not_in_embedding_space = set()

def get_glove_embedding_space():
    """
    Load a small set of GloVe word embeddings (https://nlp.stanford.edu/projects/glove/)
    """
    filename = "../file_processing/glove.6B.50d.txt"  # TODO: add file to repo (currently gitignored)
    print("Reading embedding file...")

    with open(filename, 'r') as f:
        embedding_space = {}
        for line in f:
            values = line.split()
            word = values[0]
            vector = np.asarray(values[1:], "float32")
            embedding_space[word] = vector

    return embedding_space

def get_phrase_embedding(phrase, embedding_space):
    """
    Sum embeddings for each word in `phrase` with embeddings extracted from
    `embedding_space`
    """

    separators = r'[:;,/!.\s_\-]+'
    words = re.split(separators, phrase)
    phrase_embedding = None

    for word in words:
        word = word.lower()
        if word == '':
            continue
        if word not in embedding_space:
            if word not in words_not_in_embedding_space:
                words_not_in_embedding_space.add(word)
                print(f"'{word}' not in embedding space")
            continue

        word_embedding = embedding_space[word]
        if phrase_embedding is None:
            phrase_embedding = word_embedding
        else:
            phrase_embedding += word_embedding

    return phrase_embedding

def get_schema_header_match(schema_col, csv_headers, embedding_space):
    """
    Given a single schema column name and a file's set of column headers,
    return the similarity between the schema column name and each column header
    in the file in descending similarity score order.
    """

    res = []
    schema_embed = get_phrase_embedding(schema_col, embedding_space)

    for csv_col in csv_headers:
        csv_embed = get_phrase_embedding(csv_col, embedding_space)

        if schema_embed is None and csv_embed is None:
            if schema_col == csv_col: # headers can be symbols - if exact match, assign similarity of 1
                res.append((csv_col, 1.0))

        if schema_embed is not None and csv_embed is not None:
            similarity = 1 - cosine(schema_embed, csv_embed)
            res.append((csv_col, similarity))

    return sorted(res, key = lambda tup: -tup[1])

def get_all_matches(schema_headers, csv_headers, embedding_space):
    """
    Calculate and store pairwise cosine embedding similarity between desired
    schema headers and a given CSV file's column headers.

    When both a schema and CSV header aren't words found in the embedding space,
    check if headers are exact string matches (common for technical terms)
    """

    if len(schema_headers) != len(set(schema_headers)):
        print("WARNING: duplicate column headers in schema, ")

    all_matches = defaultdict(list)

    for schema_col in schema_headers:
        if schema_col not in all_matches:
            all_matches[schema_col] = []

        schema_embed = get_phrase_embedding(schema_col, embedding_space)

        for csv_col in csv_headers:
            csv_embed = get_phrase_embedding(csv_col, embedding_space)

            if schema_embed is None and csv_embed is None:
                if schema_col == csv_col: # headers can be symbols - if exact match, assign similarity of 1
                    all_matches[schema_col].append((csv_col, 1.0))

            if schema_embed is not None and csv_embed is not None:
                similarity = 1 - cosine(schema_embed, csv_embed)
                all_matches[schema_col].append((csv_col, similarity))

    return all_matches


def get_csv_matches(schema_headers, csv_headers, embedding_space):
    """
    Return a dict mapping each schema header to its best match within `csv_headers`.
    If no match exists for a given key, None is stored.  Otherwise, values are in
    format (header: str, cosine_simarity: int)
    """

    matches = get_all_matches(schema_headers, csv_headers, embedding_space)
    best_matches = {}

    for schema_header, match_info in matches.items():
        if len(match_info) == 0:
            best_matches[schema_header] = None
        else:
            match = sorted(match_info, key = lambda info: -info[1])[0] # elt with highest cosine similarity
            best_matches[schema_header] = match

    return best_matches

def main():
    embedding_space = get_glove_embedding_space(glove_file)

    schema_headers = ["product name", "description", "price"]
    csv_headers = ["product title", "item description", "cost", "color", "weight"]

    best_matches = get_csv_matches(schema_headers, csv_headers, embedding_space)
    print('best matches:', best_matches)

if __name__ == '__main__':
    main()


