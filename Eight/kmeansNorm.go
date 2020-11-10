package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func addNormalizedStartYear() {

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

	cursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			minMaxStage})

	if err != nil {
		log.Error(err)
	}

	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {

		fmt.Println(cursor.Current)
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

	addNormalizedStartYear()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
