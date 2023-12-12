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

REGULAR = "regular"
GPT_HEADER = "gpt header"
GPT_JOIN = "gpt join"
GPT_HEADER_GPT_JOIN = "gpt header gpt join"

un_examples = [
    {
        "name": "regular UN",
        "category": "un",
        "type": REGULAR, 
        "output_file": "joined_un.csv",
        "files": [
            "Ratio of girls to boys in education.csv",
            "Seats held by women in Parliament.csv",
        ],
        "baseline_file": "baseline_un.csv",
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
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
        "name": "UN + gpt header",
        "category": "un",
        "type": GPT_HEADER,
        "output_file": "joined_un_gpt_header.csv",
        "files": [
            "GPT Header Ratio of girls to boys in education.csv",
            "GPT Header Seats held by women in Parliament.csv",
        ],
        "baseline_file": "baseline_un.csv",
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "expected_mapping": [ # schema col to file col along with the col's file src
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("Country", "", "Ratio of girls to boys in education.csv", False),
            ("Education level", "Series", "Ratio of girls to boys in education.csv", True),
            ("Gender ratio", "Value", "Ratio of girls to boys in education.csv", True),
            ("Percentage women", "Value", "Seats held by women in Parliament.csv", True)
        ], 
        "gpt_mapping": [ # file col to gpt col
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("", "Country", "Ratio of girls to boys in education.csv", False),
            ("Series", "Education Indicator", "Ratio of girls to boys in education.csv", True),
            ("Value", "Ratio", "Ratio of girls to boys in education.csv", True),
            ("Value", "Percentage", "Seats held by women in Parliament.csv", True), 
        ],
        "use_gpt_join": False,
    },
    {
        "name": "UN + gpt join",
        "category": "un",
        "type": GPT_JOIN,
        "output_file": "joined_un_gpt_join.csv",
        "files": [
            "Ratio of girls to boys in education.csv",
            "Seats held by women in Parliament.csv",
        ],
        "baseline_file": "baseline_un.csv",
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "expected_mapping": [ # schema col to file col along with the col's file src
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("Country", "", "Ratio of girls to boys in education.csv", False),
            ("Education level", "Series", "Ratio of girls to boys in education.csv", True),
            ("Gender ratio", "Value", "Ratio of girls to boys in education.csv", True),
            ("Percentage women", "Value", "Seats held by women in Parliament.csv", True)
        ], 
        "use_gpt_join": True,
    },
    {
        "name": "UN + gpt header + gpt join",
        "category": "un",
        "type": GPT_HEADER_GPT_JOIN,
        "output_file": "joined_un_gpt_header_gpt_join.csv",
        "files": [
            "GPT Header Ratio of girls to boys in education.csv",
            "GPT Header Seats held by women in Parliament.csv",
        ],
        "baseline_file": "baseline_un.csv",
        "schema_headers": [
            "Year",
            "Country",
            "Education level",
            "Gender ratio",
            "Percentage women",
        ],
        "expected_mapping": [ # schema col to file col along with the col's file src
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("Country", "", "Ratio of girls to boys in education.csv", False),
            ("Education level", "Series", "Ratio of girls to boys in education.csv", True),
            ("Gender ratio", "Value", "Ratio of girls to boys in education.csv", True),
            ("Percentage women", "Value", "Seats held by women in Parliament.csv", True)
        ], 
        "gpt_mapping": [ # file col to gpt col
            ("Year", "Year", "Ratio of girls to boys in education.csv", False),
            ("", "Country", "Ratio of girls to boys in education.csv", False),
            ("Series", "Education Indicator", "Ratio of girls to boys in education.csv", True),
            ("Value", "Ratio", "Ratio of girls to boys in education.csv", True),
            ("Value", "Percentage", "Seats held by women in Parliament.csv", True), 
        ],
        "use_gpt_join": True,
    }
]

chem_examples = [
    {
        "name": "regular chem targets", 
        "category": "chem_targets",
        "type": REGULAR,
        "output_file": "joined_chem_targets.csv",
        "files": [
            "target_components.csv",
            "target_dictionary.csv",
            "target_relations.csv",
            "target_type.csv",
        ],
        "baseline_file": "baseline_chem_targets.csv",
        "schema_headers": ["name", "target type", "desc", "component id"],
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
        "name": "chem targets + gpt header",
        "category": "chem_targets",
        "type": GPT_HEADER,
        "output_file": "joined_chem_targets_gpt_header.csv",
        "baseline_file": "baseline_chem_targets.csv",
        "files": [
            "target_components.csv",
            "target_dictionary.csv",
            "target_relations.csv",
            "target_type.csv",
        ],
        "schema_headers": ["name", "target type", "desc", "component id"],
        "expected_mapping": [
            ("name", "pref_name"),
            ("target type", "target_type"),
            ("desc", "target_desc"),
            ("component id", "component_id"),
        ],
        "gpt_mapping": [], # TODO
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
        "name": "chem targets + gpt join",
        "category": "chem_targets",
        "type": GPT_JOIN,
        "output_file": "joined_chem_targets_gpt_join.csv",
        "baseline_file": "baseline_chem_targets.csv",
        "files": [
            "target_components.csv",
            "target_dictionary.csv",
            "target_relations.csv",
            "target_type.csv",
        ],
        "schema_headers": ["name", "target type", "desc", "component id"],
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
        "name": "chem targets + gpt header + gpt join",
        "category": "chem_targets",
        "type": GPT_HEADER_GPT_JOIN,
        "output_file": "joined_chem_targets_gpt_header_gpt_join.csv",
        "baseline_file": "baseline_chem_targets.csv",
        "files": [
            "target_components.csv",
            "target_dictionary.csv",
            "target_relations.csv",
            "target_type.csv",
        ],
        "schema_headers": ["name", "target type", "desc", "component id"],
        "expected_mapping": [
            ("name", "pref_name"),
            ("target type", "target_type"),
            ("desc", "target_desc"),
            ("component id", "component_id"),
        ],
        "gpt_mapping": [], # TODO
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
]

fac_examples = [
    {
        "name": "regular fac building",
        "category": "fac_building",
        "type": REGULAR,
        "output_file": "joined_fac_building.csv",
        "files": [
            "Fac_building_address.csv",
            "Fac_building.csv",
            "Fac_floor.csv",
            "Fac_major_use.csv",
            "Fac_organization.csv",
        ],
        "baseline_file": "baseline_fac_building.csv",
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
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
        "name": "fac building + gpt header",
        "category": "fac_building", 
        "type": GPT_HEADER,
        "output_file": "joined_fac_building_gpt_header.csv",
        "files": [
            "Fac_building_address.csv",
            "Fac_building.csv",
            "Fac_floor.csv",
            "Fac_major_use.csv",
            "Fac_organization.csv",
        ],
        "baseline_file": "baseline_fac_building.csv",
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
        "expected_mapping": [
            ("building name", "Building Name"),
            ("street number", "Street Number"),
            ("street name", "Street Name"),
            ("street suffix", "Street Suffix"),
            ("rooms", "Num Of Rooms"),
        ],
        "gpt_mapping": [], # TODO
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
        "name": "fac building + gpt join",
        "category": "fac_building", 
        "type": GPT_JOIN,
        "output_file": "joined_fac_building_gpt_join.csv",
        "files": [
            "Fac_building_address.csv",
            "Fac_building.csv",
            "Fac_floor.csv",
            "Fac_major_use.csv",
            "Fac_organization.csv",
        ],
        "baseline_file": "baseline_fac_building.csv",
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
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
        "name": "fac building + gpt header + gpt join",
        "category": "fac_building", 
        "type": GPT_JOIN,
        "output_file": "joined_fac_building_gpt_join.csv",
        "files": [
            "Fac_building_address.csv",
            "Fac_building.csv",
            "Fac_floor.csv",
            "Fac_major_use.csv",
            "Fac_organization.csv",
        ],
        "baseline_file": "baseline_fac_building.csv",
        "schema_headers": [
            "building name",
            "street number",
            "street name",
            "street suffix",
            "rooms",
        ],
        "expected_mapping": [
            ("building name", "Building Name"),
            ("street number", "Street Number"),
            ("street name", "Street Name"),
            ("street suffix", "Street Suffix"),
            ("rooms", "Num Of Rooms"),
        ],
        "gpt_mapping": [], # TODO
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
]

examples = un_examples + fac_examples + chem_examples

if __name__ == "__main__":
    run_examples = [
        "regular UN",
        "UN + gpt header",
        "UN + gpt join", 
        "UN + gpt header + gpt join",
        # "regular chem targets", 
        # "chem targets + gpt header",
        # "chem targets + gpt join",
        # "chem targets + gpt header + gpt join",
        # "regular fac building",
        # "fac building + gpt header",
        # "fac building + gpt join",
        # "fac building + gpt header + gpt join"
    ]

    VERBOSE = 1
    SAVE_MAPPING = True # create pickle storing (schema_col, file_col) tups

    for example in examples:
        if example["name"] not in run_examples:
            print(f"{example['name']} not in run_examples")
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
            # pickle expected regular header mapping
            baseline_filename = PICKLE_PATH + example['category'] + '_baseline.pickle'
            with open(baseline_filename, 'wb') as f:
                print("~ pickling baseline results to", baseline_filename)
                pickle.dump(example['expected_mapping'], f)
            
            # pickle expected gpt header mapping
            if example['type'] == GPT_HEADER or example['type'] == GPT_HEADER_GPT_JOIN: 
                gpt_baseline_filename = PICKLE_PATH + example['category'] + '_baseline_gpt_header.pickle'
                with open(gpt_baseline_filename, 'wb') as f:
                    print("~ pickling baseline gpt header results to", gpt_baseline_filename)
                    pickle.dump(example['gpt_mapping'], f)

            demo_filename = PICKLE_PATH + example['output_file']
            demo_filename = demo_filename.replace('.csv', '.pickle')
            with open(demo_filename, 'wb') as f:
                print("~ pickling join results to", demo_filename)
                pickle.dump(plan['expected_mapping'], f)

        if result is None:
            print("Could not join tables")
            continue

        print("Wrote result to", OUTPUT_PATH + output_file)
        print(result.head())
        print()
        print()
