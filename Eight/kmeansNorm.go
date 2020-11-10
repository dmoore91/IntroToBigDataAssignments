package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func addNormalizedStartYear() map[string]decimal.Decimal {

	client := connectToMongo()

	filterForMoviesStage := bson.D{{"$match", bson.D{{"type", "movie"}}}}
	filterOutNoVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$ne", nil}}}}}}
	filterOutNoRating := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", nil}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}
	minMaxStage := bson.D{{"$group", bson.D{{"_id", nil},
		{"minStartYear", bson.D{{"$min", "$startYear"}}},
		{"maxStartYear", bson.D{{"$max", "$startYear"}}},
		{"minAvgRating", bson.D{{"$min", "$avgRating"}}},
		{"maxAvgRating", bson.D{{"$max", "$avgRating"}}}}}}

	cursor, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			minMaxStage})

	if err != nil {
		log.Error(err)
	}

	jsonMap := make(map[string]interface{})

	defer cursor.Close(context.Background())

	minMaxes := make(map[string]decimal.Decimal)

	for cursor.Next(context.Background()) {

		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)

		if err != nil {
			log.Error(err)
		}
	}

	minMaxes["minStartYear"], err = decimal.NewFromString(jsonMap["minStartYear"].(map[string]interface{})["$numberInt"].(string))
	if err != nil {
		log.Error(err)
	}

	minMaxes["maxStartYear"], err = decimal.NewFromString(jsonMap["maxStartYear"].(map[string]interface{})["$numberInt"].(string))
	if err != nil {
		log.Error(err)
	}

	minMaxes["minAvgRating"], err = decimal.NewFromString(jsonMap["minAvgRating"].(string))
	minMaxes["maxAvgRating"], err = decimal.NewFromString(jsonMap["maxAvgRating"].(string))

	return minMaxes
}

func addKmeansNormalized(minMaxes map[string]decimal.Decimal) {

	client := connectToMongo()

	filterForMoviesStage := bson.D{{"$match", bson.D{{"type", "movie"}}}}
	filterOutNoVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$ne", nil}}}}}}
	filterOutNoRating := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", nil}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}
	addKmeansNorm := bson.D{{"$set", bson.D{{"kmeansNorm",
		bson.A{bson.D{{"$divide",
			bson.A{bson.D{{"$subtract",
				bson.A{bson.D{{"$toDecimal", "$startYear"}}, bson.D{{"$toDecimal", minMaxes["minStartYear"].String()}}}}},
				bson.D{{"$toDecimal", minMaxes["maxStartYear"].Sub(minMaxes["minStartYear"]).String()}}}}},
			bson.A{bson.D{{"$divide",
				bson.A{bson.D{{"$subtract",
					bson.A{bson.D{{"$toDecimal", "$avgRating"}}, bson.D{{"$toDecimal", minMaxes["minAvgRating"].String()}}}}},
					bson.D{{"$toDecimal", minMaxes["maxAvgRating"].Sub(minMaxes["minAvgRating"]).String()}}}}}}}}}}}

	_, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			addKmeansNorm})

	if err != nil {
		log.Error(err)
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

	minMaxes := addNormalizedStartYear()

	addKmeansNormalized(minMaxes)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
