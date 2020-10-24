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

type fileData struct {
	BoxOfficeCurrencyLabel string              `json:"box_office_currencyLabel"`
	Title                  string              `json:"titleLabel"`
	ImdbId                 string              `json:"IMDb_ID"`
	Cost                   decimal.NullDecimal `json:"cost"`
	DistributorLabel       string              `json:"distributorLabel"`
	BoxOffice              int                 `json:"box_office"`
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
