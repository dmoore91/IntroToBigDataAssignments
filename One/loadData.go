package main

import (
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html"
	"io"
	"net/http"
	"strings"
)

func getLinks() []string {
	response, err := http.Get("https://datasets.imdbws.com/")
	if err != nil {
		log.Fatal(err)
	}

	var links []string

	tokenizer := html.NewTokenizer(response.Body)
	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			if tokenizer.Err() == io.EOF {
				break
			}
			break
		}

		str := tokenizer.Token().String()

		if strings.Contains(str, "<a") {
			if !strings.Contains(str, "title.akas.tsv.gz") {
				links = append(links, str)
			}
		}
	}

	return links
}

func main() {
	getLinks()
}
