package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type boxDataJSON struct {
	Lang  string `json:"xml:lang"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type titleJSON struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type imdbIdJSON struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type costJSON struct {
	DataType string              `json:"datatype"`
	Type     string              `json:"type"`
	Value    decimal.NullDecimal `json:"value"`
}

type distributorJSON struct {
	Lang  string `json:"xml:lang"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type boxOfficeJSON struct {
	DataType string              `json:"datatype"`
	Type     string              `json:"type"`
	Value    decimal.NullDecimal `json:"value"`
}

type fileData struct {
	BoxOfficeCurrencyLabel boxDataJSON     `json:"box_office_currencyLabel"`
	Title                  titleJSON       `json:"titleLabel"`
	ImdbId                 imdbIdJSON      `json:"IMDb_ID"`
	Cost                   costJSON        `json:"cost"`
	DistributorLabel       distributorJSON `json:"distributorLabel"`
	BoxOffice              boxOfficeJSON   `json:"box_office"`
}

type listOfFileData struct {
	Data []fileData
}

func readInJSON() {

	// read file
	file, err := os.Open("Six/extra-data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var dataList listOfFileData

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		// json data
		var f fileData

		// unmarshall it
		err = json.Unmarshal([]byte(scanner.Text()), &f)
		if err != nil {
			log.Fatal(err)
		}

		dataList.Data = append(dataList.Data, f)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {

	start := time.Now()

	readInJSON()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
