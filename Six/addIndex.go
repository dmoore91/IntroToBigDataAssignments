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

func createTitleIndex() {
	mod := mongo.IndexModel{
		Keys: bson.M{
			"title": 1, // index in descending order
		}, Options: nil,
	}

	client := connectToMongoIndex()

	_, err := client.Database("assignment_six").Collection("Movies").
		Indexes().CreateOne(context.Background(), mod)

	if err != nil {
		log.Fatal(err)
	}
}

// I am creating an index on title because updating using title_id takes forever
// however updating on id takes about 20 seconds. This leads me to believe that the
// indexing could be a factor
func main() {
	createTitleIndex()
}
