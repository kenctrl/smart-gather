import csv
import os
import openai
import pandas as pd

OPENAI_API_KEY = "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"

def get_data_sample(filepath, num_lines=20):
    with open(filepath, "r") as f:
        header = next(f)

        data = ""
        for _ in range(num_lines):
            line = next(f, None)
            if line is None:
                break
            data += line

    return header, data


def get_chat_topic(filename, header, data):
    prompt = f"""
    Here's an excerpt from a file titled {filename}.
    As you can tell, the column names within the csv aren't very precise. can you generate better ones for me given the data?
    Please return it in the same format that the header is provided in below.

    Header: {header}

    Data: {data}
    """

    client = openai.OpenAI(api_key = OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
        {"role": "system",
        "content": prompt}
        ],
        temperature=0,
        max_tokens=256
    )
    return response.choices[0].message.content


def generate_csv(gpt_headers, filepath):
    # Specify the path to your CSV file
    new_headers = gpt_headers.split(",")
    df = pd.read_csv(filepath)
    df.columns = new_headers

    gpt_filepath = os.path.splitext(filepath)[0] + "_gpt_headers.csv"
    print('new filename:', gpt_filepath)

    # Save the DataFrame back to a new CSV file or overwrite the existing one
    df.to_csv(gpt_filepath, index=False)


def main():
    filepath = "../table_joins/women_in_parliament.csv"

    header, data = get_data_sample(filepath)
    filename = os.path.basename(filepath)
    gpt_headers = get_chat_topic(filename, header, data)

    generate_csv(gpt_headers, filepath)


if __name__ == '__main__':
    main()
