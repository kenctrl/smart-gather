import csv
import pandas as pd
import random
import pickle
from evaluate_performance import EvaluatePerformance

BASELINE_PATH = "./baselines/"
PKL_PATH = "./pickles/"
GENERATED_PATH = "./generated_datasets/"

def evaluate(dataset_category):
    baseline_res = BASELINE_PATH + f"baseline_{dataset_category}.csv"
    baseline_pkl = PKL_PATH + f"{dataset_category}_baseline.pickle"

    regular_res = GENERATED_PATH + f"joined_{dataset_category}.csv"
    regular_pkl = PKL_PATH + f"joined_{dataset_category}.pickle"

    # gpt_header_res = GENERATED_PATH + f"joined_{dataset_category}_gpt_header.csv"
    # gpt_baseline_pkl = PKL_PATH + f"{dataset_category}_baseline_gpt_header.pickle"
    # gpt_header_pkl = PKL_PATH + f"joined_{dataset_category}_gpt_header.pickle"

    gpt_join_res = GENERATED_PATH + f"joined_{dataset_category}_gpt_join.csv"
    gpt_join_pkl = PKL_PATH + f"joined_{dataset_category}_gpt_join.pickle"

    # gpt_header_gpt_join_res = GENERATED_PATH + f"joined_{dataset_category}_gpt_header_gpt_join.csv"
    # gpt_header_gpt_join_pkl = PKL_PATH + f"joined_{dataset_category}_gpt_header_gpt_join.pickle"

    info = {
        "BASELINE": (baseline_res, baseline_pkl),
        "REGULAR": (regular_res, regular_pkl),
        # "GPT HEADER": (gpt_header_res, gpt_baseline_pkl, gpt_header_pkl),
        "GPT JOIN": (gpt_join_res, gpt_join_pkl),
        # "GPT HEADER GPT JOIN": (gpt_header_gpt_join_res, gpt_baseline_pkl, gpt_header_gpt_join_pkl)
    }

    evaluator = EvaluatePerformance(info)
    res = evaluator.evaluate_performance()

    print(f"~~{dataset_category} results~~")
    for k, v in res.items():
        print(f"\tcategory: {k}")
        print(f"\theader match: {v[0]}")
        print(f"\tdata match: {v[1]}")
        print()


if __name__ == '__main__':
    # evaluate("un")
    evaluate("chem_targets")
    