import time
import requests
from bs4 import BeautifulSoup

query = "state weather data".replace(" ", "+")
ACCEPTABLE_FORMATS = ["csv", "json"]
URL = f"https://catalog.data.gov/dataset/?q={query}&res_format=CSV&res_format=JSON"
print(f"Querying {URL}")

res = requests.get(URL)

soup = BeautifulSoup(res.text, "html.parser")
results = soup("div", class_="dataset-content")

output_string = ""
output_list = []  # (title, [(format, url)])
for result in results:
	title = result.find("h3").find("a").text
	s = title + "\n"
	l = []

	resources = result.find("ul", class_="dataset-resources")("a", attrs={"data-format": ACCEPTABLE_FORMATS})
	for resource in resources:
		l.append((resource["data-format"], resource["href"]))
		s += resource["href"] + "\n"

	s += "\n\n"
	if len(resources) > 0:
		output_string += s
		output_list.append((title, l))

# download the first dataset
title, links = output_list[0]
print(f"Downloading first dataset [{title}]")
for link in links:
	start_time = time.time()

	format, url = link
	print(f"- {format}: {url}", end=" ")
	data = requests.get(url)
	file = open(f"{title}.{format}", "w")
	file.write(data.text)
	file.close()

	end_time = time.time()
	elapsed_time = round(end_time - start_time, 3)
	print(f"({elapsed_time} seconds)")

# write all results to a file
file = open("results.txt", "w")
file.write(output_string)
file.close()