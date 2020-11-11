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

type actor struct {
	ActorId int      `bson:"actor" json:"actor"`
	Roles   []string `bson:"roles" json:"roles"`
}

type actorList struct {
	Actors []actor
}

type title struct {
	Id             int               `bson:"_id" json:"_id"`
	TitleType      string            `bson:"type" json:"type"`
	Title          string            `bson:"title" json:"title"`
	OriginalTitle  string            `bson:"originalTitle" json:"originalTitle"`
	StartYear      int               `bson:"startYear" json:"startYear"`
	EndYear        int               `bson:"endYear" json:"actor"`
	RuntimeMinutes int               `bson:"runtime" json:"runtime"`
	AvgRating      string            `bson:"avgRating" json:"avgRating"`
	NumVotes       int               `bson:"numVotes" json:"numVotes"`
	Genres         []string          `bson:"genres" json:"genres"`
	Actors         actorList         `bson:"actors" json:"actors"`
	Directors      []int             `bson:"directors" json:"directors"`
	Writers        []int             `bson:"writers" json:"writers"`
	Producers      []int             `bson:"producers" json:"producers"`
	KmeansNorm     []decimal.Decimal `bson:"kmeansNorm" json:"kmeansNorm"`
}

func getMinAndMax() map[string]decimal.Decimal {

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

	cursor, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes})

	defer cursor.Close(context.Background())

	var operations []mongo.WriteModel

	for cursor.Next(context.Background()) {

		var t title
		err = bson.UnmarshalExtJSON([]byte(cursor.Current.String()), false, &t)
		if err != nil {
			log.Error(err)
		}

		startYearMean := (decimal.NewFromInt(int64(t.StartYear)).Sub(minMaxes["minStartYear"])).
			Div(minMaxes["maxStartYear"].Sub(minMaxes["minStartYear"]))

		avgRatingDecimal, err := decimal.NewFromString(t.AvgRating)
		if err != nil {
			log.Error(err)
		}

		avgRatingMean := (avgRatingDecimal.Sub(minMaxes["minAvgRating"])).
			Div(minMaxes["maxAvgRating"].Sub(minMaxes["minAvgRating"]))

		t.KmeansNorm = []decimal.Decimal{startYearMean, avgRatingMean}

		operationA := mongo.NewUpdateOneModel()
		operationA.SetFilter(bson.M{"_id": t.Id})
		operationA.SetUpdate(bson.M{"$set": bson.M{"kmeansNorm": []string{startYearMean.String(), avgRatingMean.String()}}})
		operationA.SetUpsert(false)

		operations = append(operations, operationA)
	}

	_, err = client.Database("assignment_eight").Collection("Movies").
		BulkWrite(context.TODO(), operations)
	if err != nil {
		log.Fatal(err)
	}
}

func getKDocumentsFromGenre(k int, g string) {

	client := connectToMongo()

	//Clear all existing documents but just dropping collection
	err := client.Database("assignment_eight").Collection("Collections").Drop(context.Background())

	if err != nil {
		log.Error(err)
	}

	filterForMoviesStage := bson.D{{"$match", bson.D{{"type", "movie"}}}}
	filterOutNoVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$ne", nil}}}}}}
	filterOutNoRating := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", nil}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}
	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	filterForGenre := bson.D{{"$match", bson.D{{"genres", g}}}}
	sampleKRandomDocs := bson.D{{"$sample", bson.D{{"size", k}}}}

	cursor, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			unwindGenresStage, filterForGenre, sampleKRandomDocs})

	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(context.Background())

	//clusterID := 1

	for cursor.Next(context.Background()) {

		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(cursor.Current.String())
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

	//minMaxes := getMinAndMax()
	//addKmeansNormalized(minMaxes)

	getKDocumentsFromGenre(100, "Action")

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
