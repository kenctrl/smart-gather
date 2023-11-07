import requests
import csv

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
            yield csv_filename, headers


def get_best_dataset(schema_headers, scrape_result_file):
    """
    Given a list of scraped files, determine each's similarity relative to schema
    headers. Per-CSV score calculated by finding avg similarity across schema headers.
    """
    embedding_space = get_glove_embedding_space()

    all_metrics = {}
    best_filename, best_score = None, -float('inf')

    for filename, csv_headers in get_headers(scrape_result_file):
        csv_matches = get_csv_matches(schema_headers, csv_headers, embedding_space)

        score = sum([similarity for phrase, similarity in csv_matches.values()]) / len(csv_matches)

        if score > best_score:
            best_filename, best_score = filename, score

        all_metrics[filename] = {
            "score": score,
            "all": csv_matches
        }

    return best_filename, best_score, all_metrics


def main():
    scrape_results = "scrape_result_subset.txt"

    # month, day in all headers, sin in incoming_solar_final, ppt in precipitation_final
    schema_headers = ['month', 'day', 'sme2_sin_w/m2', 'sme2_ppt_mm'] 
    best_filematch, best_score, all_metrics = get_best_dataset(schema_headers, scrape_results)

    filenames_by_score = sorted(all_metrics.keys(), key=lambda filename: -all_metrics[filename]['score'])

    print()
    print(f"best file match: {best_filematch} (score: {best_score})")
    print()
    for filename in filenames_by_score:
        print("file:", filename)
        print("average score:", all_metrics[filename]['score'])
        print(all_metrics[filename]['all'])
        print()


if __name__ == '__main__':
    main()
