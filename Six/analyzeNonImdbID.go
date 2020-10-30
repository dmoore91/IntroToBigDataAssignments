package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type tvJSONAnalyze struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type xmlStructAnalyze struct {
	Lang  string `json:"xml:lang"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

type valueJSONAnalyze struct {
	DataType string              `json:"datatype"`
	Type     string              `json:"type"`
	Value    decimal.NullDecimal `json:"value"`
}

type fileDataAnalyze struct {
	BoxOfficeCurrencyLabel xmlStructAnalyze `json:"box_office_currencyLabel"`
	Title                  tvJSONAnalyze    `json:"titleLabel"`
	ImdbId                 tvJSONAnalyze    `json:"IMDb_ID"`
	Cost                   valueJSONAnalyze `json:"cost"`
	DistributorLabel       xmlStructAnalyze `json:"distributorLabel"`
	BoxOffice              valueJSONAnalyze `json:"box_office"`
	Rating                 xmlStructAnalyze `json:"MPAA_film_ratingLabel"`
}

type listOfFileDataAnalyze struct {
	Data []fileDataAnalyze
}

func readInJSONAnalyze() listOfFileDataAnalyze {

	// read file
	file, err := os.Open("Six/extra-data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var dataList listOfFileDataAnalyze

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		// json data
		var f fileDataAnalyze

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

func findMatchingTitles(numTitlesFound *uint32, numRoutinesFinished *uint32, wg *sync.WaitGroup, elem fileDataAnalyze, collection *mongo.Collection) {
	defer wg.Done()

	// update
	filter := bson.D{{"title", elem.Title.Value}}

	res, err := collection.Find(context.Background(), filter)

	if err != nil {
		log.Error(err)
	}

	for res.Next(context.Background()) {
		atomic.AddUint32(numTitlesFound, 1)
	}

	err = res.Close(context.Background())

	if err != nil {
		log.Error(err)
	}

	atomic.AddUint32(numRoutinesFinished, 1)

	//fmt.Println(*numRoutinesFinished)
}

func findUniqueTitlesAndNumMatchingInDb(data listOfFileDataAnalyze) {

	client := connectToMongoAnalyze()

	collection := client.Database("assignment_six").Collection("Movies")

	titles := mapset.NewSet()

	var numTitlesFound uint32 = 0
	var numRoutinesFinished uint32 = 0

	var wg sync.WaitGroup

	numRoutines := 8500

	for start := 0; start < len(data.Data); start += numRoutines {

		end := start + numRoutines

		if end > len(data.Data) {
			end = len(data.Data)
		}

		//Need to make this dynamic to account for last iteration which probably isn't evenly divisible by numRoutines
		wg.Add(end - start)

		//Spawn off a lot of goroutines in an attempt to make this a faster process
		for _, elem := range data.Data[start:end] {

			titles.Add(elem.Title.Value)

			go findMatchingTitles(&numTitlesFound, &numRoutinesFinished, &wg, elem, collection)
		}

		wg.Wait()

		//Keep track of where we are
		fmt.Println(end)
	}

	fmt.Println("Number of Unique Titles in extra-data.json " + strconv.Itoa(titles.Cardinality()))
	fmt.Println("Number of Total Titles in extra-data.json " + strconv.Itoa(len(data.Data)))
	fmt.Println("Number of matching documents found " + strconv.Itoa(int(numTitlesFound)))
}

func connectToMongoAnalyze() *mongo.Client {
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

	data := readInJSONAnalyze()

	findUniqueTitlesAndNumMatchingInDb(data)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
