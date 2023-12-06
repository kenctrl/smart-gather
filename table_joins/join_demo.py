import csv
import pandas as pd
import manual_join
from multi_table_join import MultiTableJoin

INPUT_PATH = "./available_datasets/"
OUTPUT_PATH = "./generated_datasets/"

examples = [
	{
		'name': '2 fake files, similar match',
		'files': ["weather.csv", "states.csv"],
		'schema_headers': ["rain", "temperature", "city"],
		'output_file': 'joined_weather.csv'
	},
	{
		'name': '3 fake files, similar match',
		'files': ["weather.csv", "states.csv", "colors.csv"],
		'schema_headers': ["rain", "temperature", "color", "capital"],
		'output_file': 'joined_colors.csv'
	},
	{
		'name': '2 real files, exact match',
		'files': ["boys_to_girls.csv", "women_in_parliment.csv"],
		'schema_headers': ["year", "country", "percentage women", "ratio girls"],
		'output_file': 'UN_dataset_join_gpt_headers.csv'
	},
]


if __name__== "__main__":
	# df1 = pd.read_csv(INPUT_PATH + "women_in_parliment.csv")
	# df2 = pd.read_csv(INPUT_PATH + "boys_to_girls.csv")
	# df3 = df1.merge(df2, left_on=["Country", "Year"], right_on=["Region/Country/Area", "Year"])
	# df3[:100][["Country", "Year", "Value_y"]].to_csv(OUTPUT_PATH + "test.csv", index=False)

	run_examples = [
		# '2 fake files, similar match',
		# '3 fake files, similar match',
		'2 real files, exact match',
	]

	VERBOSE = 2

	for example in examples:
		if example['name'] not in run_examples:
			continue

		print("=== Running", example['name'])
		print()

		files = [INPUT_PATH + filename for filename in example['files']]
		schema_headers = example['schema_headers']
		output_file = example['output_file']

		plan = manual_join.plan_join(files, schema_headers, print_results=VERBOSE >= 1)
		if plan["intersections"] is None:  # no join needed
			print("No join needed")
			continue

		join = MultiTableJoin(plan["intersections"], schema_headers, plan["files_to_matches"])
		if VERBOSE >= 2:
			print(join)

		result = join.get_result(write_to_file_name=OUTPUT_PATH + output_file, limit_rows=100)
		if result is None:
			print("Could not join tables")
			continue

		print("Wrote result to", OUTPUT_PATH + output_file)
		print(result.head())
		print()
		print()




