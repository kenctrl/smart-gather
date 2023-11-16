from er_types import *
import json
from helpers import run
from demo_weather import get_schema_headers, get_scraper_topic, get_scraper_topic_no_gpt
from data_collection.scraper import generate_scraped_urls
from file_processing.create_er_csv import generate_smart_data

'''
Given an ER schema (with entities and their relationships), produce a normalized set of tables.
'''

def create_animal_schema():
    # Attributes
    AnimalName = Attribute('Name', str)
    AnimalAge = Attribute('Age', int)
    AnimalSpecies = Attribute('Species', str)
    CageTime = Attribute('Time', int)
    CageBuilding = Attribute('Building', int)
    KeeperName = Attribute('Name', str)

    # Entities
    Animal = Entity('Animal')
    Cage = Entity('Cage')
    Keeper = Entity('Keeper')

    # Relationships (entity-entity)
    CageAnimalContains = Relationship('contains', RelationshipType.ONE_TO_MANY, Cage, Animal)
    KeeperCageKeeps = Relationship('keeps', RelationshipType.MANY_TO_MANY, Keeper, Cage)
    # Relationships (entity-attribute)
    AnimalNameName = Relationship('name', RelationshipType.ONE_TO_ONE, Animal, AnimalName)
    AnimalAgeAge = Relationship('age', RelationshipType.ONE_TO_ONE, Animal, AnimalAge)
    AnimalSpeciesSpecies = Relationship('species', RelationshipType.ONE_TO_ONE, Animal, AnimalSpecies)
    CageTimeFeedtime = Relationship('feedTime', RelationshipType.ONE_TO_ONE, Cage, CageTime)
    CageBuildingBuilding = Relationship('bldg', RelationshipType.ONE_TO_ONE, Cage, CageBuilding)
    KeeperNameName = Relationship('name', RelationshipType.ONE_TO_ONE, Keeper, KeeperName)

    # Create schema
    schema = ERSchema(
        [Animal, Cage, Keeper, AnimalName, AnimalAge, AnimalSpecies, CageTime, CageBuilding, KeeperName],
        [CageAnimalContains, KeeperCageKeeps, AnimalNameName, AnimalAgeAge, AnimalSpeciesSpecies, CageTimeFeedtime, CageBuildingBuilding, KeeperNameName]
    )

    # Create tables and ERD
    output_dir, schema_filename = run('animals', schema)
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

        # assert len(default_pk) < 2 # should only have 0-1 default primary keys per table

        return {
            'default_pk': default_pk,
            'non_default_pk': non_default_pk
        }
    

def main():
    print("=" * 80 + "\n")
    output_dir, schema_file = create_animal_schema()
    schema_headers = get_schema_headers(schema_file)
    print("Schema headers:", schema_headers)
    print()

    n_words = 3
    found_with_gpt = False
    while n_words > 1:
        topic = get_scraper_topic(schema_headers['non_default_pk'], n_words)
        generate_scraped_urls(topic)
        print("Scraper topic:", topic)
        print()
        try:
            generate_smart_data(output_dir, schema_headers, "links.txt")
            found_with_gpt = True
            break
        except:
            n_words -= 1
            print("Trying again with fewer words...")
            print()

    if not found_with_gpt:
        print("Could not find a match with GPT-4. Trying without GPT-4..\n")
        topic = get_scraper_topic_no_gpt(schema_headers['non_default_pk'])
        print("Scraper topic:", topic)
        print()
        generate_scraped_urls(topic)
        generate_smart_data(output_dir, schema_headers, "links.txt")

    print("=" * 80 + "\n")


if __name__ == '__main__':
    main()
