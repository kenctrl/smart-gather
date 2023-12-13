# Smart Gather: Intelligent and Automated Data Collection From Schema to Table

Data collection is typically a long and tedious process. Smart Gather aims to address this issue.  Given an entity relationship (ER) schema, Smart Gather normalizes the schema, scapes multiple data sources, and executes table joins to generate accurate data.

## Demo Instructions

### Setup Env
```
cd smart_gather
python -m venv smart_gather_env
source smart_gather_env/bin/activate
python -m pip install -r requirements.txt
```
If `pygraphviz` installation fails and you're on an Mac with an M series chip, try running the commands below before you retry installing the requirements:
```bash
brew install graphviz
python -m pip install \
    --global-option=build_ext \
    --global-option="-I$(brew --prefix graphviz)/include/" \
    --global-option="-L$(brew --prefix graphviz)/lib/" \
    pygraphviz
```

### Setup OpenAI API Key
Running the demo requires access to OpenAI's API.  Before running the command below, please run `export OPENAI_API_KEY=<your_key>`.

### Setup GloVE embeddings
1. Download here: https://www.kaggle.com/datasets/rtatman/glove-global-vectors-for-word-representation/
2. Unzip and move `glove.6B.50d.txt` into the `file_processing` folder.

### Run Demo
To test out our pipeline, run `python demo.py [--gpt-headers] [--gpt-join]`. The default settings use the manual pipeline without GPT augmentation. Add the `--gpt-headers` flag to view GPT-suggested header results. Add the `--gpt-join` flag to view GPT-suggested join results.

Note: It is expected that results generated with the GPT header flag perform significantly better due to the demo dataset's poor header quality.

## Pipeline Details and Code Pointers
Our pipeline can be broken down into the subtasks below.  We link the main files used to complete each portion.
1. Sourcing data (not used to produce final results but still usable to scrape data from data.gov)
   - `data_collection/scraper.py`
2. Normalized table generation
   - ER relationships: `er_types.py`
   - ER to normalized table generation: `helpers.py`
3. Determining column similarity
   - GloVE embedding and tokenization: `file_processing/utils/glove_col_similarity.py`
4. Executing table joins
   - Determine final column headers: `table_joins/manual_join.py`
   - Determine final data values when results all from single table: `table_joins/single_table_filter.py`
   - Determine final data values when table joins are necessary: `table_joins/multi_table_join.py`
5. GPT-related features
   - Headers: `table_joins/gpt_optimizations/gpt_column_headers.py`
   - Joins: `table_joins/gpt_join.py`

## Evaluation
We evaluate our pipeline's performance with and without various GPT augmentations (header generation, join execution).

Evaluation examples:
  - `table_joins/join_demo.py`

Evaluation metric implementation:
  - Metrics calculation: `table_joins/evaluate_performance.py`
  - Per dataset metrics: `table_joins/join_metrics.py`
