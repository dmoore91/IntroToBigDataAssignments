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

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

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
}

//Works
func avgRuntimeWrittenByLivingBhardwaj() {
	client := connectToMongoQuery()

	start := time.Now()

	unwindActorsStage := bson.D{{"$unwind", "$writers"}}
	joinWithMembersStage := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "writers"}, {"foreignField", "_id"}, {"as", "writer"}}}}
	getBhardwajStage := bson.D{{"$match", bson.D{{"writer.name",
		bson.D{{"$regex", "Bhardwaj"}}}}}}
	avgStage := bson.D{{"$group", bson.D{{"_id", nil}, {"avg", bson.D{{"$avg", "$runtime"}}}}}}

	showInfoCursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindActorsStage, joinWithMembersStage, getBhardwajStage, avgStage})

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	for showInfoCursor.Next(context.Background()) {
		fmt.Println(showInfoCursor.Current)
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}
}

//Works
func getSciFiMovies() {

	client := connectToMongoQuery()

	start := time.Now()

	unwindActorsStage := bson.D{{"$unwind", "$actors.actors"}}
	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	unwindDirectorsStage := bson.D{{"$unwind", "$directors"}}
	joinWithMembersStageActor := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "actors.actors.actor"}, {"foreignField", "_id"}, {"as", "actor"}}}}
	joinWithMembersStageDirector := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "directors"}, {"foreignField", "_id"}, {"as", "director"}}}}
	getSciFi := bson.D{{"$match", bson.D{{"genres", "Sci-Fi"}}}}
	getJamesCameronStage := bson.D{{"$match", bson.D{{"director.name", "James Cameron"}}}}
	getSigourneyWeaverStage := bson.D{{"$match", bson.D{{"actor._id", 244}}}}

	showInfoCursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindActorsStage, unwindGenresStage, unwindDirectorsStage, joinWithMembersStageActor,
			joinWithMembersStageDirector, getSciFi, getJamesCameronStage, getSigourneyWeaverStage})

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	for showInfoCursor.Next(context.Background()) {
		fmt.Println(showInfoCursor.Current)
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}

}

//Works
func productiveProducersNamedGil() {

	client := connectToMongoQuery()

	start := time.Now()

	unwindProducersStage := bson.D{{"$unwind", "$producers"}}
	joinWithMembersStageProducer := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "producers"}, {"foreignField", "_id"}, {"as", "producer"}}}}
	getGilStage := bson.D{{"$match", bson.D{{"producer.name",
		bson.D{{"$regex", "Gill"}}}}}}
	get2017Stage := bson.D{{"$match", bson.D{{"startYear", 2017}}}}
	addGroupAndCountStage := bson.D{{"$group", bson.D{{"_id", "$producer.name"},
		{"count", bson.D{{"$sum", 1}}}}}}
	filterOver50Stage := bson.D{{"$match", bson.D{{"count", bson.D{{"$gt", 50}}}}}}

	showInfoCursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindProducersStage, joinWithMembersStageProducer, getGilStage, get2017Stage,
			addGroupAndCountStage, filterOver50Stage})

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	for showInfoCursor.Next(context.Background()) {
		fmt.Println(showInfoCursor.Current)
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}

}

//Works
func producersWithGreatestNumberOfLongRunMovies() {
	client := connectToMongoQuery()

	start := time.Now()

	unwindProducersStage := bson.D{{"$unwind", "$producers"}}
	joinWithMembersStageProducer := bson.D{{"$lookup", bson.D{{"from", "Members"},
		{"localField", "producers"}, {"foreignField", "_id"}, {"as", "producer"}}}}
	getLongRunningMovieStage := bson.D{{"$match", bson.D{{"runtime",
		bson.D{{"$gt", 120}}}}}}
	addGroupAndCountStage := bson.D{{"$group", bson.D{{"_id", "$producer.name"},
		{"count", bson.D{{"$sum", 1}}}}}}
	sortByCountDescending := bson.D{{"$sort", bson.D{{"count", -1}}}}
	getFirstElemStage := bson.D{{"$limit", 1}}

	showInfoCursor, err := client.Database("assignment_four").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindProducersStage, joinWithMembersStageProducer, getLongRunningMovieStage,
			addGroupAndCountStage, sortByCountDescending, getFirstElemStage})

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	for showInfoCursor.Next(context.Background()) {
		fmt.Println(showInfoCursor.Current)
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}
}

func main() {
	//actorsNamedPhiAndDidntActIn2014()
	//avgRuntimeWrittenByLivingBhardwaj()
	//getSciFiMovies()
	productiveProducersNamedGil()
	//producersWithGreatestNumberOfLongRunMovies()
}
