from er_types import *
from helpers import run

'''
Given an ER schema (with entities and their relationships), produce a normalized set of tables.
'''

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
run('animals', schema)