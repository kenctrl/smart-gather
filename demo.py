import json
import openai
OPENAI_API_KEY = "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"

from er_types import *
from helpers import run

from data_collection.scraper import generate_scraped_urls
from file_processing.create_er_csv import generate_smart_data

def create_demo_schema():
    """
    Generate and save to a file the set of normalized tables corresponding
    to a weather-related ER schema.
    """

    # Attributes
    WaterYear = Attribute("water_year", int)
    CalendarYear = Attribute("calendar_year", int)
    RelativeHumidity = Attribute("sme2_rh", float)

    # Entities
    Weather = Entity("weather")

    # Relationships (entity-attribute)
    WeatherWaterYear = Relationship("waterYear", RelationshipType.ONE_TO_ONE, Weather, WaterYear)
    WeatherCalendarYear = Relationship("calendarYear", RelationshipType.ONE_TO_ONE, Weather, CalendarYear)
    WeatherRelativeHumidity = Relationship("relativeHumidity", RelationshipType.ONE_TO_ONE, Weather, RelativeHumidity)

    schema = ERSchema(
        vertices=[Weather, WaterYear, CalendarYear, RelativeHumidity],
        edges=[WeatherWaterYear, WeatherCalendarYear, WeatherRelativeHumidity]
    )

    schema_filename = run('weather', schema)
    return schema_filename


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


def get_scraper_topic(table_headers):
    """
    Given the column headers from our normalized tables as input, use OpenAI to 
    generate a topic for the scraper to search on
    """

    client = openai.OpenAI(api_key = OPENAI_API_KEY)
    gpt_input = " ".join(table_headers)

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
        {"role": "system",
        "content": "Given this database schema:\n" + gpt_input + "\n\nGenerate an on-topic phrase for the database that is less than 6 words."}
        ],
        temperature=0,
        max_tokens=256
    )

    return response.choices[0].message.content


def main():
    schema_file = create_demo_schema()
    schema_headers = get_schema_headers(schema_file)
    print("Schema headers:", schema_headers)

    topic = get_scraper_topic(schema_headers['non_default_pk'])
    print("Scraper topic:", topic)
    generate_scraped_urls(topic)
    generate_smart_data(schema_headers, "./data_collection/links.txt")


if __name__ == '__main__':
    main()
