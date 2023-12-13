import csv
import pandas as pd
import pickle
import os

class EvaluatePerformance:
    BASELINE, REGULAR, GPT_HEADER, GPT_JOIN, GPT_HEADER_GPT_JOIN = 'BASELINE', 'REGULAR', 'GPT HEADER', 'GPT JOIN', 'GPT HEADER GPT JOIN'
    CATEGORIES = [BASELINE, REGULAR, GPT_HEADER, GPT_JOIN, GPT_HEADER_GPT_JOIN]
    
    def __init__(self, pipeline_info):
        """Initializes a EvaluatePerformance object

        Args:
            data_file (str): name of base csv input file
            pipeline_info (dict):
                - key options: "BASELINE", "REGULAR", "GPT HEADERS", "GPT"
                - values: (output.csv, col_mapping.pickle) or
                          (output, col_mapping, gpt_header_mapping)
                          for BASELINE looking to compare GPT results
        """
        self.pipeline_info = pipeline_info

        self.data = self._get_data()
        self.header_mapping = self._get_header_mapping()


    def _get_df(self, filename):
        sniffer = csv.Sniffer()
        
        with open(filename, 'r', encoding='utf-8-sig') as f:
            dialect = sniffer.sniff(f.read(1024))
        f.close()
        
        df = pd.read_csv(filename, sep=dialect.delimiter)
        df.columns = [col.strip() for col in df.columns]

        for col in df.columns:
            df[col] = df[col].astype(str) # so can merge to count # rows in common
        return df


    def _get_data(self):
        """
        Create dict mapping keys in CATEGORIES set to a dataframe containing csv's data
        """
        data = {}

        for category, info in self.pipeline_info.items():
            out_csv = info[0]
            df = self._get_df(out_csv)
            data[category] = df
        
        return data


    def _get_header_mapping(self):
        """
        Create dict mapping keys in CATEGORIES set to a list of tuples (schema_col, 
        file_col)
        """
        
        header_mapping = {}

        for category, info in self.pipeline_info.items():
            # have file -> gpt col info, schema -> gpt col info, get schema -> file col info
            if category == self.GPT_HEADER or category == self.GPT_HEADER_GPT_JOIN:
                with open(info[1], 'rb') as f:
                    file_col_to_gpt_col = pickle.load(f)
                with open(info[2], 'rb') as f:
                    schema_col_to_gpt_col = pickle.load(f)

                col_info = []
                for s_col, g1_col, _ in schema_col_to_gpt_col:
                    for f_col, g2_col, src_file, _ in file_col_to_gpt_col:
                        if g1_col == g2_col:
                            col_info.append((s_col, f_col, src_file))  
            else:
                header_file = info[1]
                with open(header_file, 'rb') as f:
                    col_info = pickle.load(f)

            for ix, c in enumerate(col_info):
                modified_mapping = list(c)
                filepath = c[2]
                f = os.path.split(filepath)[1]
                modified_mapping[2] = f # set source to always just be filename
                col_info[ix] = tuple(modified_mapping)
            
            header_mapping[category] = sorted(col_info)

        return header_mapping


    def _compare_data(self, baseline_df, other_df):
        total = max(len(baseline_df), len(other_df))
        merged_df = pd.merge(baseline_df, other_df, how='inner')
        overlap = len(merged_df)

        return overlap / total
        

    def _evaluate_data_match(self):
        """
        Create dict mapping keys in CATEGORIES set to %rows of the data that 
        exactly match the baseline result
        """

        self.data_performance = {}
        baseline_df = self.data[self.BASELINE]

        for category in self.data:
            if category == self.BASELINE:
                continue

            self.data_performance[category] = self._compare_data(baseline_df, self.data[category])

    
    def _check_headers_valid(self, baseline_header, other_header):
        assert len(baseline_header) == 4 # (schema col, file col, file source, file source matters)
        assert len(other_header) == 3 # (schema col, file col, file source)
        assert baseline_header[0] == other_header[0] # schema col should always align
            

    def _compare_headers(self, baseline_headers, other_headers):
        overlap = 0
        total = len(baseline_headers)

        for baseline_header, other_header in zip(baseline_headers, other_headers):
            self._check_headers_valid(baseline_header, other_header)
            b_schema_col, b_file_col, b_src_file, check_src = baseline_header
            o_schema_col, o_file_col, o_src_file = other_header

            col_header_matches = b_file_col == o_file_col
            file_src_matches = b_src_file == o_src_file

            if (check_src and file_src_matches) and col_header_matches:
                overlap += 1
            elif not check_src and col_header_matches:
                overlap += 1

        return overlap / total


    def _evaluate_header_match(self):
        """
        Create dict mapping keys in CATEGORIES set to %headers of data that exactly 
        match the baseline result
        """

        self.header_performance = {}
        baseline_headers = self.header_mapping[self.BASELINE]

        for category, headers in self.header_mapping.items():
            if category == self.BASELINE:
                continue

            self.header_performance[category] = self._compare_headers(baseline_headers, headers)


    def evaluate_performance(self):
        """
        Returns dict with keys in CATEGORIES set mapped to tuple (%header match, %data match)
        """

        self._evaluate_data_match()
        self._evaluate_header_match()

        self.performance = {}

        for category in self.CATEGORIES:
            if category == self.BASELINE or category not in self.data:
                continue

            header_match = self.header_performance[category]
            data_match = self.data_performance[category]
            self.performance[category] = (header_match, data_match)

        return self.performance
