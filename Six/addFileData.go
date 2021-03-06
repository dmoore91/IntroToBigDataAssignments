package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/asvvvad/exchange"
	mapset "github.com/deckarep/golang-set"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
)

type tvJSON struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type xmlStruct struct {
	Lang  string `json:"xml:lang"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type valueJSON struct {
	DataType string              `json:"datatype"`
	Type     string              `json:"type"`
	Value    decimal.NullDecimal `json:"value"`
}

type fileData struct {
	BoxOfficeCurrencyLabel xmlStruct `json:"box_office_currencyLabel"`
	Title                  tvJSON    `json:"titleLabel"`
	ImdbId                 tvJSON    `json:"IMDb_ID"`
	Cost                   valueJSON `json:"cost"`
	DistributorLabel       xmlStruct `json:"distributorLabel"`
	BoxOffice              valueJSON `json:"box_office"`
	Rating                 xmlStruct `json:"MPAA_film_ratingLabel"`
}

type listOfFileData struct {
	Data []fileData
}

func readInJSON() listOfFileData {

	// read file
	file, err := os.Open("Six/extra-data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var dataList listOfFileData

	currencies := mapset.NewSet()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		// json data
		var f fileData

		// unmarshall it
		err = json.Unmarshal([]byte(scanner.Text()), &f)
		if err != nil {
			log.Fatal(err)
		}

		currencies.Add(f.BoxOfficeCurrencyLabel.Value)

		dataList.Data = append(dataList.Data, f)
	}

	fmt.Println(currencies)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return dataList
}

func updateTable(data listOfFileData) {

	ex := exchange.New("USD")
	rates, err := ex.LatestRatesAll()
	if err != nil {
		log.Fatal(err)
	}

	client := connectToMongo()

	collection := client.Database("assignment_six").Collection("Movies")

	var operations []mongo.WriteModel

	for _, elem := range data.Data {

		// Convert money to us dollars
		revenue := big.NewFloat(0)
		cost := big.NewFloat(0)

		t := new(big.Float)

		switch elem.BoxOfficeCurrencyLabel.Value {
		case "United States dollar":
			if elem.BoxOffice.Value.Valid {
				revenue = elem.BoxOffice.Value.Decimal.BigFloat()
			}

			if elem.Cost.Value.Valid {
				cost = elem.Cost.Value.Decimal.BigFloat()
			}
		case "Australian dollar":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["AUD"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["AUD"])
			}
		case "Russian ruble":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["RUB"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["RUB"])
			}
		case "pound sterling":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["GBP"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["GBP"])
			}
		case "euro":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["EUR"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["EUR"])
			}
		case "Philippine peso":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["PHP"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["PHP"])
			}
		case "Hong Kong dollar":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["HKD"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["HKD"])
			}
		case "Indian rupee":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["INR"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["INR"])
			}
		case "Thai baht":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["THB"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["THB"])
			}
		case "Czech koruna":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["CZK"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["CZK"])
			}
		case "Egyptian pound":
			if elem.BoxOffice.Value.Valid {
				revenue = t.Quo(elem.BoxOffice.Value.Decimal.BigFloat(), rates["EGP"])
			}

			if elem.Cost.Value.Valid {
				cost = t.Quo(elem.Cost.Value.Decimal.BigFloat(), rates["EGP"])
			}
		}

		rating := "No Rating"

		if elem.Rating.Value != "" {
			rating = elem.Rating.Value
		}

		distributor := "No Distributor"

		if elem.DistributorLabel.Value != "" {
			distributor = elem.DistributorLabel.Value
		}

		idStr := strings.Replace(elem.ImdbId.Value, "tt", "", 1)

		idInt, err := strconv.Atoi(idStr)
		if err != nil {
			continue
		}

		updateOP := mongo.NewUpdateManyModel()
		updateOP.SetFilter(bson.M{"_id": idInt})
		updateOP.SetUpdate(bson.M{"$set": bson.M{"distributor": distributor,
			"rating":  rating,
			"revenue": revenue,
			"cost":    cost}})
		updateOP.SetUpsert(false)
		operations = append(operations, updateOP)
	}

	// Specify an option to turn the bulk insertion in order of operation
	bulkOption := options.BulkWriteOptions{}
	bulkOption.SetOrdered(true)

	result, err := collection.BulkWrite(context.TODO(), operations, &bulkOption)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Using IMDB ID: " + strconv.Itoa(int(result.ModifiedCount)))

}

func connectToMongo() *mongo.Client {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	return client
}

func main() {

	start := time.Now()

	data := readInJSON()

	updateTable(data)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
