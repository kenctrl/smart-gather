import csv
import os
import openai
import pandas as pd
import random

OPENAI_API_KEY = "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"

def get_data_sample(filepath, num_lines=20, random_sample=False):
    """
    Get the header and `num_lines` of data from the csv
    """

    with open(filepath, "r") as f:
        lines = f.readlines()

    header = lines[0]
    data = None

    if random_sample:
        num_lines = min(num_lines, len(lines))
        indices = random.sample(range(1, len(lines)), num_lines-1)
        data = "".join([lines[i] for i in indices])
    else:
        end = min(num_lines+1, len(lines))
        data = "".join(lines[1: end])

    return header, data


def get_chat_topic(filename, header, data):
    """
    Use the filename and data to get a GPT-generated header for the file
    """

    prompt = f"""
    Here's an excerpt from a file titled "{filename}".
    As you can tell, the column names within the csv don't perfectly reflect the actual content of the data. 
    Generate a set of headers more representative of the data to replace the current header.  
    Output the answer only as a list of comma-separated values, in the order of 
    the columns that they should label, with no other output.

    Header: {header}

    Data: {data}
    """

    print("prompt:", prompt)

    client = openai.OpenAI(api_key = OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
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

    directory, filename = os.path.split(filepath)
    gpt_filepath = os.path.join(directory, "GPT HEADER " + filename)
    df.to_csv(gpt_filepath, index=False)
    
    return gpt_filepath

def generate_gpt_header(filepath):
    """
    Given a csv file, use GPT to generate a more representative set of headers
    and save the results in a new file
    """

    header, data = get_data_sample(filepath, random_sample=True)
    filename = os.path.basename(filepath)
    gpt_headers = get_chat_topic(filename, header, data)

    return generate_csv(gpt_headers, filepath)
