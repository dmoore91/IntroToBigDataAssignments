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

func readInJSONAnalyze() mapset.Set {

	// read file
	file, err := os.Open("Six/extra-data.json")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	titles := mapset.NewSet()

	numTitles := 0

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {

		// json data
		var f fileDataAnalyze

		// unmarshall it
		err = json.Unmarshal([]byte(scanner.Text()), &f)
		if err != nil {
			log.Fatal(err)
		}

		if f.Title.Value != "" {
			titles.Add(f.Title.Value)
			numTitles += 1
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Number of total titles in extra-data.json " + strconv.Itoa(numTitles))

	return titles
}

func findMatchingTitles(numTitlesFound *uint32, wg *sync.WaitGroup, title string, collection *mongo.Collection) {
	defer wg.Done()

	// update
	filter := bson.D{{"title", title}}

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
}

func findUniqueTitlesAndNumMatchingInDb(titles mapset.Set) {

	client := connectToMongoAnalyze()

	collection := client.Database("assignment_six").Collection("Movies")

	var numTitlesFound uint32 = 0

	var wg sync.WaitGroup

	wg.Add(titles.Cardinality())

	for _, title := range titles.ToSlice() {
		go findMatchingTitles(&numTitlesFound, &wg, title.(string), collection)
	}

	wg.Wait()

	fmt.Println("Number of Unique Titles in extra-data.json " + strconv.Itoa(titles.Cardinality()))
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

	titles := readInJSONAnalyze()

	findUniqueTitlesAndNumMatchingInDb(titles)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
