package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-echarts/go-echarts/charts"
	"github.com/go-echarts/go-echarts/opts"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
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

//Works
func averageRatingOfGenres() {

	client := connectToMongoQuery()

	start := time.Now()

	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	filterOutEmptyRatings := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", ""}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}
	groupByGenreStage := bson.D{{"$group", bson.D{{"_id",
		bson.D{{"genre", "$genres"}}},
		{"averageRating", bson.D{{"$avg", bson.D{{"$toDecimal", "$avgRating"}}}}}}}}

	showInfoCursor, err := client.Database("assignment_six").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindGenresStage, filterOutEmptyRatings, filterOutTooLittleVotes, groupByGenreStage})

	if err != nil {
		log.Error(err)
	}

	bar := charts.NewBar()

	// set some global options like Title/Legend/ToolTip or anything else
	bar.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title: "Average Rating for genres",
		}),
	)

	var genres []string
	var data []opts.BarData

	for showInfoCursor.Next(context.Background()) {

		var result map[string]interface{}
		err = json.Unmarshal([]byte(showInfoCursor.Current.String()), &result)
		if err != nil {
			log.Error(err)
		}

		genreMap := result["_id"].(map[string]interface{})
		genre := genreMap["genre"].(string)

		genres = append(genres, genre)

		avgRatingMap := result["averageRating"].(map[string]interface{})
		avgRating := avgRatingMap["$numberDecimal"].(string)

		avgRatingDecimal, err := decimal.NewFromString(avgRating)
		if err != nil {
			log.Error(err)
		}

		data = append(data, opts.BarData{Value: avgRatingDecimal})
	}

	// Put some data in instance
	bar.SetXAxis(genres).
		AddSeries("Category A", data)

	// iowriter
	f, err := os.Create("bar.html")
	if err != nil {
		log.Error(err)
	}

	// Where the magic happens
	err = bar.Render(f)
	if err != nil {
		log.Error(err)
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
	averageRatingOfGenres()

}
