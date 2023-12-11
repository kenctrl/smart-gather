import csv
import pandas as pd
import manual_join
import gpt_join
from gpt_optimizations.gpt_column_headers import generate_gpt_header
from single_table_filter import SingleTableFilter
from multi_table_join import MultiTableJoin
import pickle

INPUT_PATH = "./available_datasets/"
OUTPUT_PATH = "./generated_datasets/"
BASELINE_OUTPUT_PATH = "./baselines/"
PICKLE_PATH = "./pickles/"
MANUAL = "manual"
GPT_HEADER = "gpt header"

examples = [
    {
        "name": "2 fake files, similar match",
        "files": ["weather.csv", "states.csv"],
        "schema_headers": ["rain", "temperature", "city"],
        "baseline_file": "baseline_weather.csv",
        "output_file": "joined_weather.csv",
        "expected_mapping": [
            ("rain", "precipitation"), 
            ("temperature", "temperature"),
            ("city", "capital"),
        ],
        "expected_matches": {
            "weather.csv": [("precipitation", "rain"), ("temperature", "temperature")],
            "states.csv": [("capital", "city")],
        },
        "expected_joins": {("weather.csv", "states.csv"): [("state_name", "name")]},
        "use_gpt_join": False,
    },
    {
        "name": "3 fake files, similar match",
        "files": ["weather.csv", "states.csv", "colors.csv"],
        "schema_headers": ["rain", "temperature", "color", "capital"],
        "baseline_file": "baseline_colors.csv",
        "output_file": "joined_colors.csv",
        "expected_mapping": [
            ("rain", "precipitation"),
            ("temperature", "temperature"),
            ("color", "color"),
            ("capital", "capital"),
        ],
        "expected_matches": {
            "weather.csv": [("precipitation", "rain"), ("temperature", "temperature")],
            "states.csv": [("capital", "capital")],
            "colors.csv": [("color", "color")],
        },
        "expected_joins": {
            ("weather.csv", "states.csv"): [("state_name", "name")],
            ("weather.csv", "colors.csv"): [("state_name", "state")],
        },
        "use_gpt_join": False,
    },
    {
        "name": "2 modified files, exact match",
        "files": ["boys_to_girls.csv", "women_in_parliament.csv"],
        "schema_headers": ["year", "region", "name", "ratio"],
        "baseline_file": "baseline_un_dataset.csv",
        "output_file": "UN_dataset_join.csv",
        "expected_mapping": [
            ("year", "Year"),
            ("region", "Region/Country/Area"),
            ("name", "Name"),
            ("ratio", "Value"),
        ],
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
        "use_gpt_join": False,
    },
    {
        "name": "2 modified files, similar match, 1 to n column mapping, gpt header",
        "files": [
            "boys_to_girls_gpt_headers.csv",
            "women_in_parliament_gpt_headers.csv",
        ],
        "schema_headers": ["year", "country", "ratio girls", "decade"],
        "baseline_file": "baseline_un_dataset_2.csv",
        "output_file": "UN_dataset_join_gpt_header.csv",
        "expected_mapping": [
            ("year", "Year"),
            ("country", "Country or Area"),
            ("ratio girls", "Ratio of Girls to Boys"),
            ("decade", "Year"),
        ],
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
        "use_gpt_join": False,
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
        "baseline_file": "baseline_chem_targets.csv",
        "output_file": "chem_targets.csv",
        "expected_mapping": [
            ("name", "pref_name"),
            ("target type", "target_type"),
            ("desc", "target_desc"),
            ("component id", "component_id"),
        ],
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
            ("target_dictionary.csv", "target_type.csv"): [
                ("target_type", "target_type")
            ],
        },
        "use_gpt_join": False,
    },
    {
        "name": "2 real files, similar match, include unneeded files",
        "files": [
            "Fac_building_address.csv",
            "Fac_building.csv",
            "Fac_floor.csv",
            "Fac_major_use.csv",
            "Fac_organization.csv",
        ],
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
        "baseline_file": "baseline_fac_building.csv",
        "output_file": "joined_fac_building.csv",
        "expected_mapping": [
            ("building name", "Building Name"),
            ("street number", "Street Number"),
            ("street name", "Street Name"),
            ("street suffix", "Street Suffix"),
            ("rooms", "Num Of Rooms"),
        ],
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
                ("Building Key", "Fac Building Key")
            ]
        },
        "use_gpt_join": False,
    },
    {
        "name": "2 real files, specificity + similarity match",
        "category": "un",
        "type": MANUAL, 
        "files": [
            "Ratio of girls to boys in education.csv",
            "Seats held by women in Parliament.csv",
        ],
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "output_file": "un_manual.csv",
        "expected_mapping": [
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("Country", "", "Ratio of girls to boys in education.csv", False),
            ("Education level", "Series", "Ratio of girls to boys in education.csv", True),
            ("Gender ratio", "Value", "Ratio of girls to boys in education.csv", True),
            ("Percentage women", "Value", "Seats held by women in Parliament.csv", True)
        ],
        "use_gpt_join": False,
    },
    {
        "name": "2 real files, specificity + similarity match, gpt header",
        "category": "un",
        "type": GPT_HEADER,
        "files": [
            "GPT Header Ratio of girls to boys in education.csv",
            "GPT Header Seats held by women in Parliament.csv",
        ],
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "output_file": "un_gpt_header.csv",
        "expected_mapping": [ # schema col to file col along with the col's file src
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("Country", "", "Ratio of girls to boys in education.csv", False),
            ("Education level", "Series", "Ratio of girls to boys in education.csv", True),
            ("Gender ratio", "Value", "Ratio of girls to boys in education.csv", True),
            ("Percentage women", "Value", "Seats held by women in Parliament.csv", True)
        ], 
        "gpt_mapping": [ # file col to gpt col.  # TODO: should be produced by the gpt header generation file
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("", "Country", "Ratio of girls to boys in education.csv", False),
            ("Series", "Education Indicator", "Ratio of girls to boys in education.csv", True),
            ("Value", "Ratio", "Ratio of girls to boys in education.csv", True),
            ("Value", "Percentage", "Seats held by women in Parliament.csv", True), 
        ],
        "use_gpt_join": False,
    },
    {
        "name": "2 fake files, similar match, gpt join",
        "files": ["weather.csv", "states.csv"],
        "schema_headers": ["rain", "temperature", "city"],
        "baseline_file": "baseline_weather.csv",
        "output_file": "joined_weather_gpt_join.csv",
        "expected_mapping": [
            ("rain", "precipitation"),
            ("temperature", "temperature"),
            ("city", "capital"),
        ],
        "expected_matches": {
            "weather.csv": [("precipitation", "rain"), ("temperature", "temperature")],
            "states.csv": [("capital", "city")],
        },
        "expected_joins": {("weather.csv", "states.csv"): [("state_name", "name")]},
        "use_gpt_join": True,
    },
    {
        "name": "3 fake files, similar match, gpt join",
        "files": ["weather.csv", "states.csv", "colors.csv"],
        "schema_headers": ["rain", "temperature", "color", "capital"],
        "baseline_file": "baseline_colors.csv",
        "output_file": "joined_colors_gpt_join.csv",
        "expected_mapping": [
            ("rain", "precipitation"),
            ("temperature", "temperature"),
            ("capital", "capital"),
            ("color", "color"),
        ],
        "expected_matches": {
            "weather.csv": [("precipitation", "rain"), ("temperature", "temperature")],
            "states.csv": [("capital", "capital")],
            "colors.csv": [("color", "color")],
        },
        "expected_joins": {
            ("weather.csv", "states.csv"): [("state_name", "name")],
            ("weather.csv", "colors.csv"): [("state_name", "state")],
        },
        "use_gpt_join": True,
    },
    {
        "name": "2 modified files, exact match, gpt join",
        "files": ["boys_to_girls.csv", "women_in_parliament.csv"],
        "schema_headers": ["year", "region", "name", "ratio"],
        "baseline_file": "baseline_un_dataset.csv",
        "output_file": "UN_dataset_join_gpt_join.csv",
        "expected_mapping": [
            ("year", "Year"),
            ("region", "Region/Country/Area"),
            ("name", "Name"),
            ("ratio", "Value"),
        ],
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
        "use_gpt_join": True,
    },
    {
        "name": "2 modified files, similar match, 1 to n column mapping, gpt header, gpt join",
        "files": [
            "boys_to_girls_gpt_headers.csv",
            "women_in_parliament_gpt_headers.csv",
        ],
        "schema_headers": ["year", "country", "ratio girls", "decade"],
        "baseline_file": "baseline_un_dataset_2.csv",
        "output_file": "UN_dataset_join_gpt_header_gpt_join.csv",
        "expected_mapping": [
            ("year", "Year"),
            ("country", "Country or Area"),
            ("ratio girls", "Ratio of Girls to Boys"),
            ("decade", "Year"),
        ],
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
        "use_gpt_join": True,
    },
    {
        "name": "3 real files, exact match, include unneeded files, gpt join",
        "files": [
            "target_components.csv",
            "target_dictionary.csv",
            "target_relations.csv",
            "target_type.csv",
        ],
        "schema_headers": ["name", "target type", "desc", "component id"],
        "baseline_file": "baseline_chem_targets.csv",
        "output_file": "chem_targets_gpt_join.csv",
        "expected_mapping": [
            ("name", "pref_name"),
            ("target type", "target_type"),
            ("desc", "target_desc"),
            ("component id", "component_id"),
        ],
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
            ("target_dictionary.csv", "target_type.csv"): [
                ("target_type", "target_type")
            ],
        },
        "use_gpt_join": True,
    },
    {
        "name": "2 real files, similar match, include unneeded files, gpt join",
        "files": [
            "Fac_building_address.csv",
            "Fac_building.csv",
            "Fac_floor.csv",
            "Fac_major_use.csv",
            "Fac_organization.csv",
        ],
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
        "baseline_file": "baseline_fac_building.csv",
        "output_file": "joined_fac_building_gpt_join.csv",
        "expected_mapping": [
            ("building name", "Building Name"),
            ("street number", "Street Number"),
            ("street name", "Street Name"),
            ("street suffix", "Street Suffix"),
            ("rooms", "Num Of Rooms"),
        ],
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
                ("Building Key", "Fac Building Key")
            ]
        },
        "use_gpt_join": True,
    },
    {
        "name": "2 real files, specificity + similarity match, gpt join",
        "files": [
            "Ratio of girls to boys in education.csv",
            "Seats held by women in Parliament.csv",
        ],
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "output_file": "joined_women_un_metrics_gpt_join.csv",
        "use_gpt_join": True,
    },
    {
        "name": "2 real files, specificity + similarity match, gpt header, gpt join",
        "files": [
            "GPT Header Ratio of girls to boys in education.csv",
            "GPT Header Seats held by women in Parliament.csv",
        ],
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "output_file": "joined_women_un_metrics_gpt_header_gpt_join.csv",
        "use_gpt_join": True,
    },
]


if __name__ == "__main__":
    run_examples = [
        # "2 fake files, similar match",
        # "3 fake files, similar match",
        # "2 modified files, exact match",
        # "2 modified files, similar match, 1 to n column mapping, gpt header",
        # "3 real files, exact match, include unneeded files",
        # "2 real files, similar match, include unneeded files",
        # "2 real files, specificity + similarity match",
        "2 real files, specificity + similarity match, gpt header"
    ]

    VERBOSE = 1
    GPT = 0
    SAVE_MAPPING = True # create pickle storing (schema_col, file_col) tups

    for example in examples:
        if example["name"] not in run_examples:
            continue

        print("\n=== Running", example["name"])
        print()

        files = [INPUT_PATH + filename for filename in example["files"]]
        schema_headers = example["schema_headers"]
        output_file = example["output_file"]

        if example["use_gpt_join"]:
            plan = gpt_join.plan_join(files, schema_headers, verbose=VERBOSE >= 1)
        else:
            plan = manual_join.plan_join(files, schema_headers, verbose=VERBOSE >= 1)

        if plan["intersections"] is None:  # no join needed
            join = SingleTableFilter(plan["files_to_matches"], schema_headers)
        else:
            join = MultiTableJoin(
                plan["intersections"], schema_headers, plan["files_to_matches"]
            )

        if VERBOSE >= 2:
            print("Plan:", plan)
            print(join)

        result = join.get_result(
            write_to_file_name=OUTPUT_PATH + output_file, limit_rows=100
        )

        if SAVE_MAPPING:
            if example['type'] == MANUAL:
                baseline_filename = PICKLE_PATH + example['category'] + '_baseline.pickle'
                with open(baseline_filename, 'wb') as f:
                    pickle.dump(example['expected_mapping'], f)
            
            if example['type'] == GPT_HEADER:
                baseline_filename = PICKLE_PATH + example['category'] + '_baseline_gpt_header.pickle'
                with open(baseline_filename, 'wb') as f:
                    pickle.dump(example['gpt_mapping'], f)

            demo_filename = PICKLE_PATH + example['output_file']
            demo_filename = demo_filename.replace('.csv', '.pickle')
            with open(demo_filename, 'wb') as f:
                pickle.dump(plan['expected_mapping'], f)

        if result is None:
            print("Could not join tables")
            continue

        print("Wrote result to", OUTPUT_PATH + output_file)
        print(result.head())
        print()
        print()
