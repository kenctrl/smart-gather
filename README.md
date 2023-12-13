# Smart Gather: Intelligent and Automated Data Collection From Schema To Table

Data collection is typically a long and tedious process. Smart Gather aims to address this issue.  Given an entity relationship (ER) schema, Smart Gather normalizes the schema, scapes multiple data sources, and executes joins table joins to generate accurate data.

## Run Demo
To test out our pipeline, run `python demo.py [--gpt-headers] [--gpt-pipeline]`. The default settings use the manual pipeline without GPT-enhanced headers. Use the `--gpt-headers` flag to view results with those headers, and use `--gpt-pipeline` to see the results of a GPT-suggested join.
Note: it is expected behavior that the results irrespective of pipeline are worse than those produced with GPT headers enabled due to poor header quality.

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
   - Determine final data values: `table_joins/multi_table_join.py`
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
