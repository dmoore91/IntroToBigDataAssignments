package main

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func connectToMongoIndex() *mongo.Client {
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

func createStartYearIndex() {
	mod := mongo.IndexModel{
		Keys: bson.M{
			"startYear": -1, // index in descending order
		}, Options: nil,
	}

	client := connectToMongoIndex()

	_, err := client.Database("assignment_four").Collection("Movies").
		Indexes().CreateOne(context.Background(), mod)

	if err != nil {
		log.Fatal(err)
	}
}

func createIndexOnMemberName() {
	mod := mongo.IndexModel{
		Keys: bson.M{
			"name": 1, // index in descending order
		}, Options: nil,
	}

	client := connectToMongoIndex()

	_, err := client.Database("assignment_four").Collection("Members").
		Indexes().CreateOne(context.Background(), mod)

	if err != nil {
		log.Fatal(err)
	}
}

func createIndexOnRuntime() {
	mod := mongo.IndexModel{
		Keys: bson.M{
			"runtime": -1, // index in descending order
		}, Options: nil,
	}

	client := connectToMongoIndex()

	_, err := client.Database("assignment_four").Collection("Movies").
		Indexes().CreateOne(context.Background(), mod)

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	createStartYearIndex()
	createIndexOnMemberName()
	createIndexOnRuntime()
}
