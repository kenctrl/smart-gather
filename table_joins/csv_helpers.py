import csv
import pandas as pd

def read_csv(filepath, delimiter=None):
	if delimiter is None:
		sniffer = csv.Sniffer()
		with open(filepath, 'r', encoding='utf-8-sig') as f:
			dialect = sniffer.sniff(f.read(1024))
			delimiter = dialect.delimiter
		f.close()
	return pd.read_csv(filepath, sep=delimiter)
