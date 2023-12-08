import csv
import pandas as pd
class MultiTableJoin:
	def __init__(self, intersections_to_join_cols, schema_headers, files_to_cols = None):
		"""Initializes a MultiTableJoin object

		Args:
			intersections_to_join_cols (dict): {intersection: (col_1, col_2)}

			files_to_cols (dict): {file: [(col_1, schema_col_1), (col_2, schema_col_2), ...]}
		"""
		# create a dictionary {file: {other_file: (col_1, col_2)}}
		intersections = {}
		for intersection, join_cols in intersections_to_join_cols.items():
			file1, file2 = intersection
			if file1 not in intersections:
				intersections[file1] = {}
			intersections[file1][file2] = join_cols
			if file2 not in intersections:
				intersections[file2] = {}
			if isinstance(join_cols, tuple):
				intersections[file2][file1] = (join_cols[1], join_cols[0], join_cols[2])
			else:
				intersections[file2][file1] = [(jc[1], jc[0], jc[2]) for jc in join_cols]
		self.intersections = intersections

		# create a dictionary {file: {col: schema_col}}
		if files_to_cols is None:
			self.projections = None
		else:
			projections = {}
			for file, matches in files_to_cols.items():
				projections[file] = {}
				for match in matches:
					projections[file][match[0]] = match[1]
			self.projections = projections

		self.schema_headers = schema_headers

		self.dfs = {}
		self.result = None
		self.column_to_new_name = {}  # {file: {col: new_col_name}} to deal with duplicate column names

	def get_df(self, filename, can_create = True) -> pd.DataFrame:
		if filename not in self.dfs:
			if can_create:
				sniffer = csv.Sniffer()
				with open(filename, 'r') as f:
					dialect = sniffer.sniff(f.read(1024))
				f.close()
				df = pd.read_csv(filename, sep=dialect.delimiter)
				# # remove unneeded columns  NOTE: will break multi column joins in its current form
				# if self.projections is not None:
				# 	for col in df.columns:
				# 		# check if the column is needed for a join
				# 		needed_for_join = False
				# 		for other_file in self.intersections[filename]:
				# 			if col in self.intersections[filename][other_file]:
				# 				needed_for_join = True
				# 				break
				# 		if col not in self.projections[filename] and not needed_for_join:
				# 			del df[col]
				self.dfs[filename] = df
			else:
				return None
		return self.dfs[filename]

	def get_table_name(self, filename):
		return filename.split('/')[-1].split('.')[0]

	def get_current_column_name(self, filename, col):
		if filename in self.column_to_new_name and col in self.column_to_new_name[filename]:
			return self.column_to_new_name[filename][col]
		return col

	def distinguish_column_name(self, filename, col, df):
		if filename not in self.column_to_new_name:
			self.column_to_new_name[filename] = {}
		if col not in self.column_to_new_name[filename]:
			new_col_name = self.get_table_name(filename) + '_' + col
			self.column_to_new_name[filename][col] = new_col_name
			df.rename(columns={col: new_col_name}, inplace=True)

	def get_result(self, write_to_file_name=None, limit_rows=None) -> pd.DataFrame:
		if isinstance(self.result, str):
			print(self.result)
			return None
		elif self.result is not None:
			if write_to_file_name is not None:
				result = self.result
				if limit_rows is not None and len(result) > limit_rows:
					result = result[:limit_rows]
				result.to_csv(write_to_file_name, index=False)
			return self.result

		seen_files = set()
		seen_columns = []
		result = None

		expected_num_files = len(self.intersections)

		all_files = list(self.intersections.keys())
		for file in all_files:
			if file not in self.intersections:  # already finished all joins for this file
				continue

			if result is None:
				result = self.get_df(file)
				seen_files.add(file)
				seen_columns.append((file, result.columns))

			if file not in seen_files:
				continue

			other_files = list(self.intersections[file].keys())
			for other_file in other_files:
				other_df = self.get_df(other_file)

				# check for duplicate column names
				if other_file not in seen_files:
					for col in other_df.columns:
						found_duplicate = False
						for seen_file, seen_file_columns in seen_columns:
							if col in seen_file_columns:
								self.distinguish_column_name(seen_file, col, result)
								found_duplicate = True
						if found_duplicate:
							self.distinguish_column_name(other_file, col, other_df)

					seen_columns.append((other_file, other_df.columns))

				# join the two tables
				join_cols = self.intersections[file][other_file]

				if isinstance(join_cols, tuple):  # single column join
					join_cols = [join_cols]
				left_cols = [self.get_current_column_name(file, jc[0]) for jc in join_cols]
				right_cols = [self.get_current_column_name(other_file, jc[1]) for jc in join_cols]

				# rank the join columns by cardinality
				i = 0
				cols_ranked = []
				while i < len(left_cols):
					# skip if column types don't match
					if result[left_cols[i]].dtype != other_df[right_cols[i]].dtype:
						i += 1
						continue

					# ignore empty rows in the join columns
					left_df = result.dropna(axis=0, how="any", subset=[left_cols[i]])
					right_df = other_df.dropna(axis=0, how="any", subset=[right_cols[i]])

					# find the number of rows in the join
					card = len(left_df.merge(right_df, left_on=[left_cols[i]], right_on=[right_cols[i]], how='inner'))

					if card > 0:
						cols_ranked.append((card, i))

					i += 1

				# if no columns on which to join, fail
				if len(cols_ranked) == 0:
					error = f"ERROR: no columns on which to join {file} and {other_file}"
					print(error)
					self.result = error
					return None

				# choose up to 2 columns to join on, ranked by length of resulting dataframe
				cols_ranked = [jc[1] for jc in sorted(
					cols_ranked,
					key=lambda col: (col[0], join_cols[col[1]][2]),
					reverse=True
				)]
				if len(cols_ranked) > 2:
					cols_ranked = cols_ranked[:2]

				left_cols = [jc for i, jc in enumerate(left_cols) if i in cols_ranked]
				right_cols = [jc for i, jc in enumerate(right_cols) if i in cols_ranked]
				print("joining", file, "and", other_file, "on", left_cols, "and", right_cols)
				print()

				# do the join
				result = result.merge(other_df, left_on=left_cols, right_on=right_cols, how='inner')

				seen_files.add(other_file)

				del self.intersections[other_file][file]
				del self.intersections[file][other_file]

			# remove the file from memory if we're done with it
			if len(self.intersections[file]) == 0:
				del self.intersections[file]
				del self.dfs[file]

		# check if we've seen all the files
		if len(seen_files) != expected_num_files:
			print("ERROR: expected to see", expected_num_files, "files, but only saw", len(seen_files))
			print("seen files:", seen_files)
			print("expected files:", all_files)

			self.result = "ERROR: joins did not form a connected graph"
			return None

		# project the result
		if self.projections is not None:
			projections = {}
			for file in self.projections:
				for col in self.projections[file]:
					projections[self.get_current_column_name(file, col)] = self.projections[file][col]
			print("projecting to", projections)
			result.rename(columns=projections, inplace=True)
			print("result columns:", result.columns)
			print("and we want:", self.schema_headers)
			result = result[self.schema_headers]

		self.result = result

		if limit_rows is not None and len(result) > limit_rows:
			result = result[:limit_rows]

		if write_to_file_name is not None:
			result.to_csv(write_to_file_name, index=False)

		return result

	def __str__(self) -> str:
		s = f"MultiTableJoin:\n"
		s += f"intersections={self.intersections},\n"
		s += f"projections={self.projections}\n"
		return s
