import csv
import time
import pandas as pd

def get_headers(filenames):
    headers_per_file = {}
    for filename in filenames:
        print("filename:", filename)
        with open(filename, 'r') as file:
            csv_reader = csv.reader(file, delimiter="\n")
            header = next(csv_reader)[0].split(',')
            header = [col.replace('\ufeff', '') for col in header]
            print("Headerrrr", header)
            headers_per_file[filename] = header

    print("headers per file:", headers_per_file)
    return headers_per_file

def find_header_intersection(header_info):
    keys = [k for k in header_info.keys()]
    res = {}

    for i in range(len(keys)):
        filename1 = keys[i]
        for j in range(i+1, len(keys)):
            filename2 = keys[j]
            header1 = header_info[filename1]
            header2 = header_info[filename2]
            
            shared = set(header1).intersection(set(header2))
            res[(filename1, filename2)] = shared

    return res

def join_tables(desired_headers, intersection, result_filename):
    er_info = {}
    for file1, file2 in intersection:
        for filename in [file1, file2]:
                df = pd.read_csv(filename)
                columns = df.to_dict(orient='list')
                for cname in columns:
                    if cname in desired_headers:
                        er_info[cname] = columns[cname]

    with open(result_filename, 'w', newline='') as f:
        # Create a CSV writer object
        csv_writer = csv.writer(f)
        # Write the header (column names)
        csv_writer.writerow(er_info.keys())
        # Write the data
        csv_writer.writerows(zip(*er_info.values()))

    print("er columns:", er_info)

if __name__ == '__main__':
    filenames = ['day_id_state.csv', 'day_id_temp_precip.csv']
    headers = get_headers(filenames)
    intersection = find_header_intersection(headers)

    desired_headers = ["day_id", "state", "temperature (degrees C)", "precipitation level"]
    result_filename = "tester.csv"
    join_tables(desired_headers, intersection, result_filename)

    print("intersection:", intersection)