# Daniel Moore
# 9/10/2020
# This program will automatically open up https://datasets.imdbws.com/, download the files and them to my local
# postgres database

import requests
from bs4 import BeautifulSoup


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


if __name__ == "__main__":
    getLinks()
