import time
import requests
from bs4 import BeautifulSoup

import json
import csv


def generate_scraped_urls(topic):
	ACCEPTABLE_FORMATS = ["csv"]

	query = topic.replace(" ", "+")  # "state weather data".replace(" ", "+")
	params = f"q={query}"
	for format in ACCEPTABLE_FORMATS:
		params += f"&res_format={format.upper()}"
	URL = f"https://catalog.data.gov/dataset/?{params}"
	print(f"Querying {URL}")

	res = requests.get(URL)

	soup = BeautifulSoup(res.text, "html.parser")
	results = soup("div", class_="dataset-content")

	# -- write all CSV links to a file --
	file = open("links.txt", "w")
	links = []
	for result in results:
		resources = result.find("ul", class_="dataset-resources")("a", attrs={"data-format": ACCEPTABLE_FORMATS})
		for resource in resources:
			links.append(resource["href"])
	file.write("\n".join(links))
	file.close()

	return file


# -- download the CSV files reachable from the first page --
# output_string = ""
# output_list = []  # (title, [(format, url)])
# for result in results:
# 	title = result.find("h3").find("a").text
# 	s = title + "\n"
# 	l = []

# 	resources = result.find("ul", class_="dataset-resources")("a", attrs={"data-format": ACCEPTABLE_FORMATS})
# 	for resource in resources:
# 		l.append((resource["data-format"], resource["href"]))
# 		s += resource["href"] + "\n"

# 	s += "\n\n"
# 	if len(resources) > 0:
# 		output_string += s
# 		output_list.append((title, l))

# write all results to a file
# file = open("results.txt", "w")
# file.write(output_string)
# file.close()


# -- download the first dataset --

# title, links = output_list[0]
# print(f"Downloading first dataset [{title}]")
# for link in links:
# 	start_time = time.time()

# 	format, url = link

# 	# retrieve data
# 	print(f"- {format}: {url}", end=" ")
# 	data = requests.get(url)
# 	file = open(f"{title}.{format}", "w")
# 	file.write(data.text)
# 	file.close()

# 	end_time = time.time()
# 	elapsed_time = round(end_time - start_time, 3)
# 	print(f"({elapsed_time} seconds)")


def read_column_headers_from_csv(csv_string):
	reader = csv.reader(csv_string.split("\n"), delimiter=",")
	return next(reader)


def json_to_csv(json_string):
	data = json.loads(json_string)
	csv_data = csv.writer(open("data.csv", "w"))

	columns = data["meta"]["view"]["columns"]
	csv_data.writerow([column["name"] for column in columns])

	for row in data["data"]:
		csv_data.writerow(row)


# with open("Electric Vehicle Population Data.json", "r") as data:
# 	json_to_csv(data.read())
