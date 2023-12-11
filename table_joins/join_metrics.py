import csv
import pandas as pd
import random
import pickle
from evaluate_performance import EvaluatePerformance

BASELINE_PATH = "./baselines/"
PKL_PATH = "./pickles/"
GENERATED_PATH = "./generated_datasets/"

if __name__ == '__main__':
    # evaluate UN dataset
    baseline_res = BASELINE_PATH + "baseline_un.csv"
    baseline_pkl = PKL_PATH + "un_baseline.pickle"

    manual_res = GENERATED_PATH + "un_manual.csv"
    manual_pkl = PKL_PATH + "un_manual.pickle"

    gpt_header_res = GENERATED_PATH + "un_gpt_header.csv"
    gpt_baseline_pkl = PKL_PATH + "un_baseline_gpt_header.pickle"
    gpt_header_pkl = PKL_PATH + "un_gpt_header.pickle"

    info = {
        "BASELINE": (baseline_res, baseline_pkl),
        "MANUAL": (manual_res, manual_pkl),
        "GPT HEADER": (gpt_header_res, gpt_baseline_pkl, gpt_header_pkl)
    }

    un_eval = EvaluatePerformance(info)
    res = un_eval.evaluate_performance()

    print("~~UN results~~")
    for k, v in res.items():
        print(f"\tcategory: {k}")
        print(f"\theader match: {v[0]}")
        print(f"\tdata match: {v[1]}")
        print()
