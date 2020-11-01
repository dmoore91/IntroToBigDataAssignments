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
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
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

		if err := p.Save(8.5*vg.Inch, 11*vg.Inch, "Six/"+genre+"_box.png"); err != nil {
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

func main() {
	averageRatingOfGenres()

}
