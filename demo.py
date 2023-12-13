import json
import openai
import sys
import os
from dotenv import load_dotenv

import sys
import os

sys.path.append(os.getcwd() + "/table_joins")

# Now you can import your module
from table_joins.multi_table_join import MultiTableJoin
from table_joins.single_table_filter import SingleTableFilter
from table_joins import manual_join, gpt_join

load_dotenv()
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

from er_types import *
from helpers import run

from data_collection.scraper import generate_scraped_urls
from file_processing.create_er_csv import generate_smart_data

DATA_PATH = "./demo_data/"
OUTPUT_PATH = "./demo_results/"

def create_demo_schema():
    """
    Generate and save to a file the set of normalized tables.
    """

    # Attributes
    Country = Attribute("Country", str)
    Year = Attribute("Year", int)
    EducationLevel = Attribute("Education level", float)
    GenderRatio = Attribute("Gender ratio", float)
    PercentageWomen = Attribute("Percentage women", float)

    # Entities
    CountryYearData = Entity("CountryYearData")

    # Relationships (entity-attribute)
    CYDCountry = Relationship("Country", RelationshipType.ONE_TO_MANY, CountryYearData, Country)
    CYDYear = Relationship("Year", RelationshipType.ONE_TO_MANY, CountryYearData, Year)
    CYDGenderRatio = Relationship("Gender ratio", RelationshipType.ONE_TO_MANY, CountryYearData, GenderRatio)
    CYDEducationLevel = Relationship("Education level", RelationshipType.ONE_TO_MANY, CountryYearData, EducationLevel)
    CYDPercentageWomen = Relationship("Percentage women", RelationshipType.ONE_TO_MANY, CountryYearData, PercentageWomen)

    schema = ERSchema(
        vertices=[CountryYearData, Country, Year, EducationLevel, GenderRatio, PercentageWomen],
        edges=[CYDCountry, CYDYear, CYDGenderRatio, CYDEducationLevel, CYDPercentageWomen]
    )

    output_dir, schema_filename = run('demo', schema)
    return output_dir, schema_filename


def get_schema_headers(schema_file):
    """
    Currently only supports one normalized table - return its column headers
    """

    with open(schema_file, 'rb') as file:
        schema = json.load(file)

    if schema:
        first_table = next(iter(schema))
        col_headers = list(schema[first_table].keys())

        default_pk = [elt for elt in col_headers if elt.endswith("_id")]
        non_default_pk = [elt for elt in col_headers if not elt.endswith("_id")]

        assert len(default_pk) < 2 # should only have 0-1 default primary keys per table

        return {
            'default_pk': default_pk,
            'non_default_pk': non_default_pk
        }


def get_scraper_topic_no_gpt(table_headers):
    """
    Given the column headers from our normalized tables as input,
    generate a topic for the scraper to search on
    """

    return " ".join(table_headers)


def get_scraper_topic(table_headers, n_words=6):
    """
    Given the column headers from our normalized tables as input, use OpenAI to
    generate a topic for the scraper to search on
    """

    client = openai.OpenAI(api_key = OPENAI_API_KEY)
    gpt_input = " ".join(table_headers)

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
        {"role": "system",
        "content": "Given this database schema:\n" + gpt_input + f"\n\nGenerate an on-topic phrase for the database that is less than {n_words} words."}
        ],
        temperature=0,
        max_tokens=256
    )

    return response.choices[0].message.content


def main():
    flags = sys.argv[1:]

    print("=" * 80 + "\n")
    _, schema_file = create_demo_schema()
    schema_headers = get_schema_headers(schema_file)["non_default_pk"]
    print("Schema headers:", schema_headers)
    print()

    topic = get_scraper_topic(schema_headers)
    print("Scraper topic:", topic)
    print()

    files = ["Ratio of girls to boys in education.csv", "Seats held by women in Parliament.csv"]
    if "--gpt-headers" in flags:
        files = [f"GPT Header {file}" for file in files]
    files = [DATA_PATH + file for file in files]

    print(f"(Skipping scraping stage - using pre-downloaded data: {files})")
    print()

    if "--gpt-join" in flags:
        plan = gpt_join.plan_join(files, schema_headers, verbose=False)
    else:
        plan = manual_join.plan_join(files, schema_headers, verbose=True)


    if plan["intersections"] is None:  # no join needed
        join = SingleTableFilter(plan["files_to_matches"], schema_headers)
    else:
        join = MultiTableJoin(
            plan["intersections"], schema_headers, plan["files_to_matches"]
        )

    # filename indicates set of flags used to generate output
    identifiers = "pipeline_"
    if len(flags) == 0: # running just base_pipeline
        identifiers = "base_" + identifiers
    if "--gpt-join" in flags:
        identifiers += "w_gpt_join_"
    if "--gpt-headers" in flags:
        identifiers += "w_gpt_headers_"

    if not os.path.exists(OUTPUT_PATH):
      os.mkdir(OUTPUT_PATH)

    output_file = OUTPUT_PATH + identifiers + "output.csv"
    result = join.get_result(
        write_to_file_name=output_file, limit_rows=None
    )

    print(f"Result (saved to {output_file}):")
    print(result)

    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()
