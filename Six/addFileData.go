package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

	return dataList
}

func addWithImdbID(data listOfFileData) {

	client := connectToMongo()

	collection := client.Database("assignment_six").Collection("Movies")

	numChanged := 0

	for _, elem := range data.Data {

		//Should do currency conversions for day movie was released

		var revenue decimal.Decimal
		var cost decimal.Decimal

		if elem.BoxOfficeCurrencyLabel.Value == "United States dollar" {

			if elem.BoxOffice.Value.Valid {
				revenue = elem.BoxOffice.Value.Decimal
			}

			if elem.Cost.Value.Valid {
				cost = elem.Cost.Value.Decimal
			}
		}

		idStr := strings.ReplaceAll(elem.ImdbId.Value, "tt", "")

		idInt, err := strconv.Atoi(idStr)
		if err != nil {
			log.Error(err)
		}

		result, err := collection.UpdateOne(
			context.Background(),
			bson.M{"_id": idInt},
			bson.D{
				{"$set", bson.D{{"distributor", elem.DistributorLabel.Value},
					{"rating", elem.Rating.Value},
					{"revenue", revenue},
					{"cost", cost}}},
			})

		if err != nil {
			log.Fatal(err)
		}

		numChanged += int(result.ModifiedCount)
	}
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

	addWithImdbID(data)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
