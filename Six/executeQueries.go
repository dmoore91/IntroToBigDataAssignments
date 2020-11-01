package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/wcharczuk/go-chart"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
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

func averageRatingOfGenres() {

	client := connectToMongoQuery()

	start := time.Now()

	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	filterOutEmptyRatings := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", ""}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}

	showInfoCursor, err := client.Database("assignment_six").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{unwindGenresStage, filterOutEmptyRatings, filterOutTooLittleVotes})

	if err != nil {
		log.Error(err)
	}

	genreMap := make(map[string]plotter.Values)

	for showInfoCursor.Next(context.Background()) {

		var result map[string]interface{}
		err = json.Unmarshal([]byte(showInfoCursor.Current.String()), &result)
		if err != nil {
			log.Error(err)
		}

		genre := result["genres"].(string)

		avgRating := result["avgRating"].(string)

		avgRatingDecimal, err := decimal.NewFromString(avgRating)
		if err != nil {
			log.Error(err)
		}

		tmp, _ := avgRatingDecimal.Float64()

		genreMap[genre] = append(genreMap[genre], tmp)
	}

	for genre, data := range genreMap {

		p, err := plot.New()
		if err != nil {
			panic(err)
		}
		p.Title.Text = genre

		box, err := plotter.NewBoxPlot(vg.Length(15), 0.0, data)
		if err != nil {
			log.Error(err)
		}
		p.Add(box)

		if err := p.Save(8.5*vg.Inch, 11*vg.Inch, "Six/"+genre+"_box.jpg"); err != nil {
			panic(err)
		}
	}

	err = showInfoCursor.Close(context.Background())

	if err != nil {
		log.Error(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

}

func averageNumberOfActorsForGenres() {

	client := connectToMongoQuery()

	start := time.Now()

	filterOutNoActors := bson.D{{"$match", bson.D{{"actors.actors",
		bson.D{{"$ne", nil}}}}}}
	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	groupByGenreStage := bson.D{{"$group", bson.D{{"_id",
		bson.D{{"genre", "$genres"}}},
		{"averageNumberOfActors", bson.D{{"$avg", bson.D{{"$size", "$actors.actors"}}}}}}}}
	showInfoCursor, err := client.Database("assignment_six").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterOutNoActors, unwindGenresStage, groupByGenreStage})

	if err != nil {
		log.Error(err)
	}

	var barList []chart.Value

	for showInfoCursor.Next(context.Background()) {

		var result map[string]interface{}
		err = json.Unmarshal([]byte(showInfoCursor.Current.String()), &result)
		if err != nil {
			log.Error(err)
		}

		idMap := result["_id"].(map[string]interface{})
		genre := idMap["genre"].(string)

		avgActorMap := result["averageNumberOfActors"].(map[string]interface{})
		numActors := avgActorMap["$numberDouble"].(string)

		avgRatingDecimal, err := decimal.NewFromString(numActors)
		if err != nil {
			log.Error(err)
		}

		tmp, _ := avgRatingDecimal.Float64()

		barList = append(barList, chart.Value{
			Label: genre,
			Value: tmp,
		})
	}

	graph := chart.BarChart{
		Title: "Average Number Of Actors",
		Background: chart.Style{
			Padding: chart.Box{
				Top: 40,
			},
		},
		Width:    3000,
		BarWidth: 250,
		Bars:     barList,
	}

	f, _ := os.Create("Six/numActors.png")

	defer f.Close()
	err = graph.Render(chart.PNG, f)
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
	//averageRatingOfGenres()
	averageNumberOfActorsForGenres()

}
