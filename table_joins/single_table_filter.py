import csv
import pandas as pd
import random

class SingleTableFilter:
    def __init__(self, file_mapping, schema_headers):
        """Initializes a SingleTableFilter object

        Args:
            file_mapping (dict): {filename : [(file_col, schema_col), ...]}
            schema_headers (list): list of requested headers for schema
        """

        assert(len(file_mapping) == 1)

        self.filename = list(file_mapping.keys())[0]
        self.schema_headers = schema_headers
        
        self.headers = {}
        for f_col, s_col in file_mapping[self.filename]:
            self.headers[f_col] = self.headers.get(f_col, []) + [s_col]

        self.get_df()
    
    def get_df(self) -> pd.DataFrame:
        """
        Create raw df from csv file
        """

        sniffer = csv.Sniffer()
        with open(self.filename, 'r', encoding='utf-8-sig') as f:
            dialect = sniffer.sniff(f.read(1024))
        f.close()
        df = pd.read_csv(self.filename, sep=dialect.delimiter)

        # trim whitespace from headers
        df.columns = [col.strip() for col in df.columns]
        self.df = df

        headers = self.df.columns.tolist()
        print("original headers", headers)
            
        return self.df

    def _replace_col(self, row) -> None:
        """
        Given a row of the input file df, create a dict mapping its values into
        the schema format
        """
        
        new_row = {}
        for file_col, schema_cols in self.headers.items():
            for schema_col in schema_cols:
                new_row[schema_col] = row[file_col]
        
        return new_row

    def get_result(self, write_to_file_name=None, limit_rows=None, verbose=False) -> pd.DataFrame:
        """
        Create a df with the schema headers populated with input csv data. Save 
        `limit_rows` rows if specified by `write_to_file_name`.
        """

        self.df = pd.DataFrame(self.df.apply(self._replace_col, axis=1).tolist())
        
        headers = self.df.columns.tolist()
        print("headers after rename", headers)

        self.result = self.df.drop_duplicates()
        self.result = self.result[self.schema_headers]

        if limit_rows is not None and len(self.result) > limit_rows:
            self.result = self.result[:limit_rows]


        if write_to_file_name is not None:
            self.result.to_csv(write_to_file_name, index=False)

        return self.result
