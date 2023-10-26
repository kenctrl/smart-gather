#editor.detectIndentation#
from er_types import *
import json
import subprocess
import erdantic as erd
import importlib
import os
import time

def run(name: str, schema: Schema):
    run_time = time.time_ns()
    os.chdir('./output')
    if not os.path.exists(name):
      os.mkdir(name)
    os.chdir(name)
    if not os.path.exists(f'{run_time}'):
      os.mkdir(f'{run_time}')
    os.chdir(f'{run_time}')
    json_schema = schema.create_json_schema()
    print(json_schema)

    # Save json file
    with open(f'{name}_schema.json', 'w') as f:
        json.dump(json_schema, f, indent=4)

    # Save pydantic models
    subprocess.run(['datamodel-codegen', '--input-file-type', 'json', '--snake-case-field', '--input', f'{name}_schema.json', '--output', f'{name}_models.py', '--class-name', 'TABLE'])

    # Save erdantic diagram
    package_name = f'output.{name}.{run_time}'
    animals_models = importlib.import_module(f'.{name}_models', package=package_name)
    erd.draw(animals_models.TABLE, out=f'{name}_diagram.png')