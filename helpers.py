#editor.detectIndentation#
from er_types import *
import json
import subprocess
import erdantic as erd
import importlib
import os
import time

def create_schema(er_schema: ERSchema) -> Dict[str, Table]:
    tables: Dict[str, Table] = {}
    seen: Set[str] = set()

    for vertex in er_schema.vertices:
        if vertex.object_type == ObjectType.ENTITY:
            if vertex.name not in seen:
                tables[vertex.name] = Table(vertex.name, [Column(f"{vertex.name.lower()}_id", int, KeyType.PRIMARY)])
                seen.add(vertex.name)
            else:
                raise Exception(f'Entity {vertex.name} already exists')

    for edge in er_schema.edges:
        if edge.parent.object_type == ObjectType.ENTITY and edge.parent.name in tables:
            if edge.child.object_type == ObjectType.ENTITY:
                if edge.type == RelationshipType.ONE_TO_ONE:
                    tables[edge.parent.name].columns.append(Column(f"{edge.child.name.lower()}_id", int, KeyType.FOREIGN))
                elif edge.type == RelationshipType.ONE_TO_MANY:
                    tables[edge.child.name].columns.append(Column(f"{edge.parent.name.lower()}_id", int, KeyType.FOREIGN))
                elif edge.type == RelationshipType.MANY_TO_ONE:
                    tables[edge.parent.name].columns.append(Column(f"{edge.child.name.lower()}_id", int, KeyType.FOREIGN))
                elif edge.type == RelationshipType.MANY_TO_MANY: # create new table
                    new_table_name = f'{edge.parent.name}_{edge.child.name}'
                    if new_table_name not in seen:
                        tables[new_table_name] = Table(new_table_name, [
                            Column(f"{edge.parent.name.lower()}_id", int, KeyType.FOREIGN),
                            Column(f"{edge.child.name.lower()}_id", int, KeyType.FOREIGN)
                        ])
                        seen.add(new_table_name)
                    else:
                        raise Exception(f'Table {new_table_name} already exists')
                else:
                    raise Exception(f'Unknown relationship type {edge.type}')
            elif edge.child.object_type == ObjectType.ATTRIBUTE:
                tables[edge.parent.name].columns.append(Column(edge.child.name, edge.child.type))
            else:
                raise Exception(f'Unknown object type {edge.child.object_type}')
        else:
            raise Exception(f'Input error: attributes should be children of entities')

    return tables

def create_json_schema(er_schema: ERSchema) -> Dict[str, Dict]:
    tables = create_schema(er_schema)
    json_schema: Dict[str, Dict] = {}
    for table in tables.values():
        json_schema[table.name] = {}
        for column in table.columns:
            json_schema[table.name][column.name] = column.type.__name__
    return json_schema

def print_schema(er_schema: ERSchema):
    result = '=' * 80 + '\n'
    result += 'ERSchema:\n'
    tables = create_schema(er_schema)
    for table in tables.values():
        result += f'{table}\n'
    result += '=' * 80 + '\n'
    print(result)

def print_entity_relations(er_schema: ERSchema):
    result = '=' * 80 + '\n'
    result += 'Vertices:\n'
    for vertex in er_schema.vertices:
        result += f'- {vertex.name}\n'
    result += '\n'
    result += 'Edges:\n'
    for edge in er_schema.edges:
        result += f'- {edge}\n'
    result += '=' * 80 + '\n'
    print(result)

def run(schema_name: str, er_schema: ERSchema):
    timestamp = time.time_ns()
    OUTPUT_DIR = f"./er_output/{schema_name}/{timestamp}"
    print("output dir:", OUTPUT_DIR)
    FILEPATH = f"{OUTPUT_DIR}/{schema_name}_schema.json"
    print("filepath:", FILEPATH)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    json_schema = create_json_schema(er_schema)
    print("Generated schema:", json_schema)
    print()

    # Save json file
    with open(FILEPATH, 'w') as f:
        json.dump(json_schema, f, indent=4)

    # Save pydantic models
    # subprocess.run(['datamodel-codegen', '--input-file-type', 'json', '--snake-case-field', '--input', f'{schema_name}_schema.json', '--output', f'{schema_name}_models.py', '--class-schema_name', 'DATASET'])

    # Save erdantic diagram
    # package_schema_name = f'output.{schema_name}.{run_time}'
    # animals_models = importlib.import_module(f'.{schema_name}_models', package=package_schema_name)
    # erd.draw(animals_models.DATASET, out=f'{schema_name}_diagram.png')

    return (OUTPUT_DIR, FILEPATH)
