import requests
import csv
import time

from utils.glove_col_similarity import *

def get_headers(scrape_result_file):
    """
    Given a list of CSVs from scraping, extract and yield headers from each file
    """
    with open(scrape_result_file, 'r') as f:
        for line in f:
            url = line.strip()
            csv_filename = url.split("/")[-1]
            res = requests.get(url)

            if res.status_code != 200:
                print(f"unable to get file {csv_filename}")
                continue

            content = res.text
            csv_data = list(csv.reader(content.splitlines()))
            headers = csv_data[0]

            print(f"headers for file {csv_filename}: {headers}")
            yield csv_filename, url, headers


def get_best_dataset(schema_headers, scrape_result_file):
    """
    Given a list of scraped files, determine each's similarity relative to schema
    headers. Per-CSV score calculated by finding avg similarity across schema headers.
    """
    embedding_space = get_glove_embedding_space()

    all_metrics = {}
    best_filename, best_url, best_score = None, None, -float('inf')

    for filename, url, csv_headers in get_headers(scrape_result_file):
        csv_matches = get_csv_matches(schema_headers, csv_headers, embedding_space)

        score = sum([similarity for phrase, similarity in csv_matches.values()]) / len(csv_matches)

        if score > best_score:
            best_filename, best_url, best_score = filename, url, score

        all_metrics[filename] = {
            "score": score,
            "url": url,
            "all": csv_matches
        }
    
    return best_filename, best_url, best_score, all_metrics


def get_indices(schema_headers, mapping, file_headers):
    """
    Get the indices of columns within file_headers in the order they come in
    schema_headers. 

    ex: schema_headers = ['a', 'b', 'c]
        mapping = {'a': ('a': 1), 'b': ('bb': 0.75), 'c': ('c', 1)}
        file_headers = ['bb', 'a', 'c']
        results in [1, 0, 2]
    """
    indices = []

    for schema_header in schema_headers:
        mapped_header = mapping[schema_header][0]
        for ix, file_header in enumerate(file_headers):
            if mapped_header == file_header:
                indices.append(ix)

    return indices


def create_er_csv(schema_headers, best_url, best_col_info, max_rows=50):
    """
    Given the schema's headers, the file containing the best match, and a mapping
    of schema header name to its best match in the provided file, create a csv
    containing the requested headers.

    best_col_info: {
        schema column (str): (csv column (str), similarity (float))
    }
    """
    output_filename = f"schema_{time.time()}.csv"

    res = requests.get(best_url.strip())
    if res.status_code != 200:
        print(f"unable to get best file url {csv_filename}")

    content = res.text
    csv_data = list(csv.reader(content.splitlines()))
    file_headers = csv_data[0]

    ordered_header_indices = get_indices(schema_headers, best_col_info, file_headers)

    er_rows = []
    for row in csv_data[1:]:
        er_row = {schema_headers[schema_ix]: row[file_ix] for schema_ix, file_ix in enumerate(ordered_header_indices)}
        er_rows.append(er_row)

    # Write the selected rows to the output CSV file
    with open(output_filename, "w", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=schema_headers)
        writer.writeheader()
        writer.writerows(er_rows)

    print(f"CSV file with requested headers has been created at {output_filename}.")


def main():
    scrape_results = "scrape_result_subset.txt"

    # month, day in all headers, sin in incoming_solar_final, ppt in precipitation_final
    # schema_headers = ['month', 'day', 'sme2_sin_w/m2', 'sme2_ppt_mm'] 
    schema_headers = ['water_year', 'calendar_year', 'sme2_rh']
    best_filematch, best_url, best_score, all_metrics = get_best_dataset(schema_headers, scrape_results)

    filenames_by_score = sorted(all_metrics.keys(), key=lambda filename: -all_metrics[filename]['score'])

    print()
    print(f"best file match: {best_filematch} (score: {best_score})")
    print()
    for filename in filenames_by_score:
        print("file:", filename)
        print("average score:", all_metrics[filename]['score'])
        print(all_metrics[filename]['all'])
        print()
    
    create_er_csv(schema_headers, best_url, all_metrics[best_filematch]['all'])


if __name__ == '__main__':
    main()
