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

func connectToMongoQuery() *mongo.Client {
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

func actorsNamedPhiAndDidntActIn2014() {

	client := connectToMongoQuery()

	start := time.Now()

	unwindActorsStage := bson.D{{"$unwind", "$actors.actors"}}
	joinWithMembersStage := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "actors.actors.actor"}, {"foreignField", "_id"}, {"as", "actor_id"}}}}
	filterOutDeadActorsStage := bson.D{{"$match", bson.D{{"actor_id.deathYear", 0}}}}
	filterOutDeadActorsStagePartTwoElectricBoogaloo := bson.D{{"$match", bson.D{{"actor_id.deathYear", nil}}}}

	showInfoCursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindActorsStage, joinWithMembersStage, filterOutDeadActorsStage,
			filterOutDeadActorsStagePartTwoElectricBoogaloo})

	if err != nil {
		log.Error(err)
	}

	count := 0
	max := 1

	for showInfoCursor.Next(context.Background()) {
		if count < max {
			fmt.Println(showInfoCursor.Current)
		}

		count += 1
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")
}

func avgRuntimeWrittenByLivingBhardwaj() {
	client := connectToMongoQuery()

	start := time.Now()

	unwindActorsStage := bson.D{{"$unwind", "$writers"}}
	joinWithMembersStage := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "writers"}, {"foreignField", "_id"}, {"as", "writer"}}}}
	getBhardwajStage := bson.D{{"$match", bson.D{{"writer.name",
		bson.D{{"$regex", "Bhardwaj"}}}}}}
	//$project: { quizAvg: { $avg: "$quizzes"}
	avgStage := bson.D{{"$group", bson.D{{"_id", nil}, {"avg", bson.D{{"$avg", "$runtime"}}}}}}

	showInfoCursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindActorsStage, joinWithMembersStage, getBhardwajStage, avgStage})

	if err != nil {
		log.Error(err)
	}

	for showInfoCursor.Next(context.Background()) {
		fmt.Println(showInfoCursor.Current)
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")
}

func main() {
	//actorsNamedPhiAndDidntActIn2014()
	avgRuntimeWrittenByLivingBhardwaj()
}