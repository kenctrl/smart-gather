# # Given an ER schema (with entities and their relationships), produce a normalized set of tables.

# import sys
# import os
# import re
# import json
# import copy
# import pprint
# import argparse
# import logging
# import logging.config
# import yaml
# import er_schema

# # Set up logging
# logging.config.fileConfig('logging.conf')
# logger = logging.getLogger('er_to_schema')

# # Set up command line arguments
# parser = argparse.ArgumentParser(description='Given an ER schema (with entities and their relationships), produce a normalized set of tables.')
# parser.add_argument('er_schema_file', help='ER schema file')
# parser.add_argument('output_file', help='Output file')
# parser.add_argument('--debug', action='store_true', help='Debug mode')
# args = parser.parse_args()

# # Set up debug mode
# if args.debug:
#     logger.setLevel(logging.DEBUG)

# # Read in the ER schema
# er_schema_file = args.er_schema_file
# logger.info('Reading in ER schema from %s', er_schema_file)
# with open(er_schema_file, 'r') as f:
#     er_schema = json.load(f)

# # Create a new schema
# schema = {
#     'entities': [],
#     'relationships': [],
#     'tables': [],
#     'table_names': [],
#     'table_map': {}
# }

# # Create a new entity for each entity in the ER schema
# for entity in er_schema['entities']:
#     logger.debug('Creating entity %s', entity['name'])
#     new_entity = er_schema.Entity(entity['name'], entity['attributes'])
#     schema['entities'].append(new_entity)

# # Create a new relationship for each relationship in the ER schema
# for relationship in er_schema['relationships']:
#     logger.debug('Creating relationship %s', relationship['name'])
#     new_relationship = er_schema.Relationship(relationship['name'], relationship['entities'], relationship['attributes'])
#     schema['relationships'].append(new_relationship)

# # Create a new table for each entity in the ER schema
# for entity in schema['entities']:
#     logger.debug('Creating table %s', entity.name)
#     new_table = er_schema.Table(entity.name, entity.attributes)
#     schema['tables'].append(new_table)
#     schema['table_names'].append(entity.name)
#     schema['table_map'][entity.name] = new_table

# # Create a new table for each relationship in the ER schema
# for relationship in schema['relationships']:
#     logger.debug('Creating table %s', relationship.name)
#     new_table = er_schema.Table(relationship.name, relationship.attributes)
#     schema['tables'].append(new_table)
#     schema['table_names'].append(relationship.name)
#     schema['table_map'][relationship.name] = new_table

# # Create a new table for each many-to-many relationship in the ER schema
# for relationship in schema['relationships']:
#     if relationship.is_many_to_many():
#         logger.debug('Creating table %s', relationship.name)
#         new_table = er_schema.Table(relationship.name, relationship.attributes)
#         schema['tables'].append(new_table)
#         schema['table_names'].append(relationship.name)
#         schema['table_map'][relationship.name] = new_table
        