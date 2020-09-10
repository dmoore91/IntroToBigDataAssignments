# Daniel Moore
# 9/10/2020
# This program will automatically open up https://datasets.imdbws.com/, download the files and them to my local
# postgres database

import requests
from bs4 import BeautifulSoup
from io import StringIO
import gzip


# This function will be used to open https://datasets.imdbws.com/ and get all the links to the files that need
# to be read in
def getLinks():

    text = requests.get("https://datasets.imdbws.com/").text

    soup = BeautifulSoup(text, "html.parser")

    listItems = soup.find_all("ul")

    links = []

    for item in listItems:
        link = item.a['href']

        if "akas" not in link:
            links.append(link)

    return links


def readFileFromLink(link):
    text = requests.get(link).text
    stringFP = StringIO(text)

    with gzip.open(stringFP, 'rb') as f:
        file_content = f.read()

    print(file_content)


if __name__ == "__main__":
    # getLinks()
    readFileFromLink("https://datasets.imdbws.com/name.basics.tsv.gz")
