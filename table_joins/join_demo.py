import csv
import pandas as pd
import manual_join
import gpt_join
from multi_table_join import MultiTableJoin

INPUT_PATH = "./available_datasets/"
OUTPUT_PATH = "./generated_datasets/"

examples = [
    {
        "name": "2 fake files, similar match",
        "files": ["weather.csv", "states.csv"],
        "schema_headers": ["rain", "temperature", "city"],
        "output_file": "joined_weather.csv",
        "expected_matches": {
            "weather.csv": [("precipitation", "rain"), ("temperature", "temperature")],
            "states.csv": [("capital", "city")],
        },
        "expected_joins": {("weather.csv", "states.csv"): [("state_name", "name")]},
    },
    {
        "name": "3 fake files, similar match",
        "files": ["weather.csv", "states.csv", "colors.csv"],
        "schema_headers": ["rain", "temperature", "color", "capital"],
        "output_file": "joined_colors.csv",
        "expected_matches": {
            "weather.csv": [("precipitation", "rain"), ("temperature", "temperature")],
            "states.csv": [("capital", "capital")],
            "colors.csv": [("color", "color")],
        },
        "expected_joins": {
            ("weather.csv", "states.csv"): [("state_name", "name")],
            ("weather.csv", "colors.csv"): [("state_name", "state")],
        },
    },
    {
        "name": "2 real files, exact match",
        "files": ["boys_to_girls.csv", "women_in_parliament.csv"],
        "schema_headers": ["year", "region", "name", "ratio"],
        "output_file": "UN_dataset_join.csv",
        "expected_matches": {
            "boys_to_girls.csv": [
                ("Year", "year"),
                ("Region/Country/Area", "region"),
                ("Value", "ratio"),
            ],
            "women_in_parliament.csv": [("Name", "name")],
        },
        "expected_joins": {
            ("boys_to_girls.csv", "women_in_parliament.csv"): [
                ("Region/Country/Area", "Region/Country/Area"),
                ("Year", "Year"),
            ]
        },
    },
    {
        "name": "2 real files, similar match, 1 to n column mapping",
        "files": [
            "boys_to_girls_gpt_headers.csv",
            "women_in_parliament_gpt_headers.csv",
        ],
        "schema_headers": ["year", "country", "ratio girls", "decade"],
        "output_file": "UN_dataset_join_2.csv",
        "expected_matches": {
            "boys_to_girls_gpt_headers.csv": [
                ("Year", "year"),
                ("Ratio of Girls to Boys", "ratio girls"),
                ("Year", "decade"),
            ],
            "women_in_parliament_gpt_headers.csv": [("Country or Area", "country")],
        },
        "expected_joins": {
            ("boys_to_girls_gpt_headers.csv", "women_in_parliament_gpt_headers.csv"): [
                ("Region/Country/Area", "Country or Area"),
                ("Year", "Year"),
            ]
        },
    },
    {
        "name": "3 real files, exact match, include unneeded files",
        "files": [
            "target_components.csv",
            "target_dictionary.csv",
            "target_relations.csv",
            "target_type.csv",
        ],
        "schema_headers": ["name", "target type", "desc", "component id"],
        "output_file": "chem_targets.csv",
        "expected_matches": {
            "target_components.csv": [("component_id", "component id")],
            "target_dictionary.csv": [("pref_name", "name")],
            "target_type.csv": [
                ("target_desc", "desc"),
                ("target_type", "target type"),
            ],
        },
        "expected_joins": {
            ("target_components.csv", "target_dictionary.csv"): [("tid", "tid")],
            ("target_type.csv", "target_dictionary.csv"): [
                ("target_type", "target_type")
            ],
        },
    },
    {
        "name": "2 real files, similar match",
        "files": ["Fac_building_address.csv", "Fac_building.csv"],
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
        "output_file": "joined_fac_building.csv",
        "expected_matches": {
            "Fac_building_address.csv": [
                ("Street Number", "street number"),
                ("Street Name", "street name"),
                ("Street Suffix", "street suffix"),
            ],
            "Fac_building.csv": [
                ("Building Name", "building name"),
                ("Num Of Rooms", "rooms"),
            ],
        },
        "expected_joins": {
            ("Fac_building_address.csv", "Fac_building.csv"): [
                ("Fac Building Key", "Building Key")
            ]
        },
    },
]


if __name__ == "__main__":
    run_examples = [
        "2 fake files, similar match",
        "3 fake files, similar match",
        "2 real files, exact match",
        "2 real files, similar match, 1 to n column mapping",
        "3 real files, exact match, include unneeded files",
        "2 real files, similar match",
    ]

    VERBOSE = 1
    GPT = 0

    for example in examples:
        if example["name"] not in run_examples:
            continue

        print("=== Running", example["name"])
        print()

        files = [INPUT_PATH + filename for filename in example["files"]]
        schema_headers = example["schema_headers"]
        output_file = example["output_file"]

        if GPT:
            plan = gpt_join.plan_join(files, schema_headers, verbose=VERBOSE >= 1)
        else:
            plan = manual_join.plan_join(
                files, schema_headers, verbose=VERBOSE >= 1
            )

        if plan["intersections"] is None:  # no join needed
            print("No join needed")
            continue
        if VERBOSE >= 2:
            print("Plan:", plan)

        join = MultiTableJoin(
            plan["intersections"], schema_headers, plan["files_to_matches"]
        )
        if VERBOSE >= 2:
            print(join)

        result = join.get_result(
            write_to_file_name=OUTPUT_PATH + output_file, limit_rows=100
        )
        if result is None:
            print("Could not join tables")
            continue

        print("Wrote result to", OUTPUT_PATH + output_file)
        print(result.head())
        print()
        print()
