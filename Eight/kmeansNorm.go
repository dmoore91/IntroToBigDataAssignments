package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
	"math/rand"
	"os"
	"strconv"
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

type cluster struct {
	Id              int     `bson:"_id"`
	KmeansStartYear float64 `bson:"kmeansStartYear"`
	KmeansAvgRating float64 `bson:"kmeansAvgRating"`
}

type floats struct {
	AvgRating float64
	StartYear float64
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
	err := client.Database("assignment_eight").Collection("centroids").Drop(context.Background())

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

	cursor, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			unwindGenresStage, filterForGenre})

	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(context.Background())

	var clusters []cluster

	for cursor.Next(context.Background()) {

		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)
		if err != nil {
			log.Fatal(err)
		}

		var c cluster
		c.KmeansStartYear, err = strconv.ParseFloat(jsonMap["kmeansNorm"].([]interface{})[0].(string), 64)
		if err != nil {
			log.Fatal(err)
		}

		c.KmeansAvgRating, err = strconv.ParseFloat(jsonMap["kmeansNorm"].([]interface{})[1].(string), 64)
		if err != nil {
			log.Fatal(err)
		}

		clusters = append(clusters, c)

	}

	// Needed to make sure we get different random number each time
	rand.Seed(time.Now().UnixNano())

	rand.Shuffle(len(clusters), func(i, j int) {
		clusters[i], clusters[j] = clusters[j], clusters[i]
	})

	var operations []mongo.WriteModel

	for idx, c := range clusters[:k] {

		c.Id = idx + 1

		operationA := mongo.NewInsertOneModel()
		operationA.SetDocument(c)

		operations = append(operations, operationA)
	}

	_, err = client.Database("assignment_eight").Collection("centroids").
		BulkWrite(context.TODO(), operations)
	if err != nil {
		log.Fatal(err)
	}

}

func oneStepKMeans(g string) int {

	client := connectToMongo()

	// Get our clusters

	cursor, err := client.Database("assignment_eight").Collection("centroids").Find(context.Background(), bson.D{})

	if err != nil {
		log.Error(err)
	}

	var clusters []cluster

	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)
		if err != nil {
			log.Fatal(err)
		}

		avgRating, err := strconv.ParseFloat(jsonMap["kmeansAvgRating"].(map[string]interface{})["$numberDouble"].(string), 64)
		if err != nil {
			log.Fatal(err)
		}

		startYear, err := strconv.ParseFloat(jsonMap["kmeansStartYear"].(map[string]interface{})["$numberDouble"].(string), 64)
		if err != nil {
			log.Fatal(err)
		}

		id, err := strconv.Atoi(jsonMap["_id"].(map[string]interface{})["$numberInt"].(string))
		if err != nil {
			log.Fatal(err)
		}

		c := cluster{
			Id:              id,
			KmeansStartYear: startYear,
			KmeansAvgRating: avgRating,
		}

		clusters = append(clusters, c)
	}

	//Get documents to cluster

	filterForMoviesStage := bson.D{{"$match", bson.D{{"type", "movie"}}}}
	filterOutNoVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$ne", nil}}}}}}
	filterOutNoRating := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", nil}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}
	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	filterForGenre := bson.D{{"$match", bson.D{{"genres", g}}}}

	cursor, err = client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			unwindGenresStage, filterForGenre})

	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(context.Background())

	clusterToKmeansMap := make(map[int][]floats)

	for cursor.Next(context.Background()) {
		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)
		if err != nil {
			log.Fatal(err)
		}

		kmeansNorm := jsonMap["kmeansNorm"].([]interface{})

		startYear, _ := strconv.ParseFloat(kmeansNorm[0].(string), 64)
		avgRating, _ := strconv.ParseFloat(kmeansNorm[1].(string), 64)

		clusterID := 0
		minDistance := math.MaxFloat64

		for _, c := range clusters {

			startYearDistance := c.KmeansStartYear - startYear
			avgRatingDistance := c.KmeansAvgRating - avgRating

			startYearDistanceSquared := math.Pow(startYearDistance, 2)
			avgRatingDistanceSquared := math.Pow(avgRatingDistance, 2)

			distance := math.Sqrt(startYearDistanceSquared + avgRatingDistanceSquared)

			if distance <= minDistance {
				minDistance = distance
				clusterID = c.Id
			}
		}

		clusterToKmeansMap[clusterID] = append(clusterToKmeansMap[clusterID], floats{
			AvgRating: avgRating,
			StartYear: startYear,
		})

		id, err := strconv.Atoi(jsonMap["_id"].(map[string]interface{})["$numberInt"].(string))
		if err != nil {
			log.Fatal(err)
		}

		// update
		filter := bson.D{{"_id", id}}

		update := bson.D{{"$set", bson.D{{"cluster", clusterID}}}}

		_, err = client.Database("assignment_eight").Collection("Movies").UpdateOne(
			context.Background(), filter, update)

		if err != nil {
			log.Fatal(err)
		}
	}

	numClusters := 0
	numUpdated := 0
	for cluster, kmeans := range clusterToKmeansMap {

		avgRatingSum := 0.0
		startYearSum := 0.0

		for _, k := range kmeans {
			avgRatingSum += k.AvgRating
			startYearSum += k.StartYear
		}

		avgAvgRating := avgRatingSum / float64(len(kmeans))
		avgStartYear := startYearSum / float64(len(kmeans))

		// update
		filter := bson.D{{"_id", cluster}}

		update := bson.D{{"$set", bson.D{{"kmeansStartYear", avgStartYear},
			{"kmeansAvgRating", avgAvgRating}}}}

		result, err := client.Database("assignment_eight").Collection("centroids").UpdateOne(
			context.Background(), filter, update)

		if err != nil {
			log.Fatal(err)
		}

		numClusters += 1
		numUpdated += int(result.ModifiedCount)
	}

	return numUpdated
}

func getSumOfSquaredErrors(g string) float64 {

	client := connectToMongo()

	// Get our clusters

	cursor, err := client.Database("assignment_eight").Collection("centroids").Find(context.Background(), bson.D{})

	if err != nil {
		log.Error(err)
	}

	clustermap := make(map[int]cluster)

	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)
		if err != nil {
			log.Fatal(err)
		}

		avgRating, err := strconv.ParseFloat(jsonMap["kmeansAvgRating"].(map[string]interface{})["$numberDouble"].(string), 64)
		if err != nil {
			log.Fatal(err)
		}

		startYear, err := strconv.ParseFloat(jsonMap["kmeansStartYear"].(map[string]interface{})["$numberDouble"].(string), 64)
		if err != nil {
			log.Fatal(err)
		}

		id, err := strconv.Atoi(jsonMap["_id"].(map[string]interface{})["$numberInt"].(string))
		if err != nil {
			log.Fatal(err)
		}

		c := cluster{
			Id:              id,
			KmeansStartYear: startYear,
			KmeansAvgRating: avgRating,
		}

		clustermap[id] = c
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

	cursor, err = client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			unwindGenresStage, filterForGenre})

	if err != nil {
		log.Error(err)
	}

	defer cursor.Close(context.Background())

	sse := 0.0

	for cursor.Next(context.Background()) {

		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)

		if err != nil {
			log.Error(err)
		}

		kmeansStartYear, err := strconv.ParseFloat(jsonMap["kmeansNorm"].([]interface{})[0].(string), 64)

		if err != nil {
			log.Error(err)
		}

		kmeansAvgRating, err := strconv.ParseFloat(jsonMap["kmeansNorm"].([]interface{})[1].(string), 64)
		if err != nil {
			log.Error(err)
		}

		clusterId, err := strconv.Atoi(jsonMap["cluster"].(map[string]interface{})["$numberInt"].(string))
		if err != nil {
			log.Error(err)
		}

		cluster := clustermap[clusterId]

		startYearErrorSquared := math.Pow(kmeansStartYear-cluster.KmeansStartYear, 2)
		avgRatingErrorSquared := math.Pow(kmeansAvgRating-cluster.KmeansAvgRating, 2)

		sse += startYearErrorSquared
		sse += avgRatingErrorSquared
	}

	return sse
}

func runKMeansOnGenresAndSizes() {

	genres := []string{"Action", "Horror", "Romance", "Sci-Fi", "Thriller"}

	for _, genre := range genres {

		var data []float64
		var clusterSizes []float64

		for k := 10; k <= 50; k += 5 {
			getKDocumentsFromGenre(k, genre)

			for i := 0; i < 100; i++ {
				numUpdated := oneStepKMeans(genre)

				if numUpdated == 0 {
					//At this point we've converged and nothing will change the more we run kmeans
					break
				}
			}

			//Print initial Graph
			graph := visualizeCluster(genre)

			f, _ := os.Create("Eight/Scatterplots/" + genre + "/" + genre + "_" + strconv.Itoa(k) + ".png")

			err := graph.Render(chart.PNG, f)
			if err != nil {
				log.Error(err)
			}

			err = f.Close()
			if err != nil {
				log.Error(err)
			}

			sse := getSumOfSquaredErrors(genre)
			data = append(data, sse)

			clusterSizes = append(clusterSizes, float64(k))
		}

		graph := chart.Chart{
			Series: []chart.Series{
				chart.ContinuousSeries{
					XValues: clusterSizes,
					YValues: data,
				},
			},
		}
		f, _ := os.Create("Eight/" + genre + ".png")
		err := graph.Render(chart.PNG, f)
		if err != nil {
			log.Error(err)
		}
		err = f.Close()
		if err != nil {
			log.Error(err)
		}
	}
}

func visualizeCluster(g string) chart.Chart {

	client := connectToMongo()

	filterForMoviesStage := bson.D{{"$match", bson.D{{"type", "movie"}}}}
	filterOutNoVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$ne", nil}}}}}}
	filterOutNoRating := bson.D{{"$match", bson.D{{"avgRating",
		bson.D{{"$ne", nil}}}}}}
	filterOutTooLittleVotes := bson.D{{"$match", bson.D{{"numVotes",
		bson.D{{"$gt", 10000}}}}}}
	unwindGenresStage := bson.D{{"$unwind", "$genres"}}
	filterForGenre := bson.D{{"$match", bson.D{{"genres", g}}}}

	cursor, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			unwindGenresStage, filterForGenre})

	if err != nil {
		log.Error(err)
	}

	defer cursor.Close(context.Background())

	clustermap := make(map[floats]int)

	var xValues []float64
	var yValues []float64

	for cursor.Next(context.Background()) {

		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)

		c, _ := strconv.Atoi(jsonMap["cluster"].(map[string]interface{})["$numberInt"].(string))
		startYear, _ := strconv.ParseFloat(jsonMap["kmeansNorm"].([]interface{})[0].(string), 64)
		avgRating, _ := strconv.ParseFloat(jsonMap["kmeansNorm"].([]interface{})[1].(string), 64)

		f := floats{
			AvgRating: avgRating,
			StartYear: startYear,
		}

		clustermap[f] = c

		xValues = append(xValues, startYear)
		yValues = append(yValues, avgRating)
	}

	colorLookup := make(map[int]drawing.Color)
	colorLookup[1] = drawing.ColorFromHex("#f032e6")
	colorLookup[2] = drawing.ColorFromHex("#a9a9a9")
	colorLookup[3] = drawing.ColorFromHex("#fabed4")
	colorLookup[4] = drawing.ColorFromHex("#ffd8b1")
	colorLookup[5] = drawing.ColorFromHex("#fffac8")
	colorLookup[6] = drawing.ColorFromHex("#aaffc3")
	colorLookup[7] = drawing.ColorFromHex("#dcbeff")
	colorLookup[8] = drawing.ColorFromHex("#ff0000")
	colorLookup[9] = drawing.ColorFromHex("#730000")
	colorLookup[10] = drawing.ColorFromHex("#99574d")
	colorLookup[11] = drawing.ColorFromHex("#e56739")
	colorLookup[12] = drawing.ColorFromHex("#33201a")
	colorLookup[13] = drawing.ColorFromHex("#b39286")
	colorLookup[14] = drawing.ColorFromHex("#ffa640")
	colorLookup[15] = drawing.ColorFromHex("#99754d")
	colorLookup[16] = drawing.ColorFromHex("#332200")
	colorLookup[17] = drawing.ColorFromHex("#bfa330")
	colorLookup[18] = drawing.ColorFromHex("#d6e600")
	colorLookup[19] = drawing.ColorFromHex("#535900")
	colorLookup[20] = drawing.ColorFromHex("#eef2b6")
	colorLookup[21] = drawing.ColorFromHex("#525943")
	colorLookup[22] = drawing.ColorFromHex("#4cd936")
	colorLookup[23] = drawing.ColorFromHex("#004000")
	colorLookup[24] = drawing.ColorFromHex("#00730f")
	colorLookup[25] = drawing.ColorFromHex("#30bf8f")
	colorLookup[26] = drawing.ColorFromHex("#16594c")
	colorLookup[27] = drawing.ColorFromHex("#005266")
	colorLookup[28] = drawing.ColorFromHex("#79daf2")
	colorLookup[29] = drawing.ColorFromHex("#0080bf")
	colorLookup[30] = drawing.ColorFromHex("#1a2b33")
	colorLookup[31] = drawing.ColorFromHex("#163159")
	colorLookup[32] = drawing.ColorFromHex("#002999")
	colorLookup[33] = drawing.ColorFromHex("#000e33")
	colorLookup[34] = drawing.ColorFromHex("#acbbe6")
	colorLookup[35] = drawing.ColorFromHex("#434959")
	colorLookup[36] = drawing.ColorFromHex("#6674cc")
	colorLookup[37] = drawing.ColorFromHex("#0a004d")
	colorLookup[38] = drawing.ColorFromHex("#4100f2")
	colorLookup[39] = drawing.ColorFromHex("#ba79f2")
	colorLookup[40] = drawing.ColorFromHex("#311a33")
	colorLookup[41] = drawing.ColorFromHex("#fbbfff")
	colorLookup[42] = drawing.ColorFromHex("#ff40f2")
	colorLookup[43] = drawing.ColorFromHex("#731d6d")
	colorLookup[44] = drawing.ColorFromHex("#ff40a6")
	colorLookup[45] = drawing.ColorFromHex("#664d57")
	colorLookup[46] = drawing.ColorFromHex("#d9003a")
	colorLookup[47] = drawing.ColorFromHex("#33000e")
	colorLookup[48] = drawing.ColorFromHex("#994d61")
	colorLookup[49] = drawing.ColorFromHex("#ff8091")
	colorLookup[50] = drawing.ColorFromHex("#f2b6be")

	viridisByY := func(xr, yr chart.Range, index int, x, y float64) drawing.Color {

		f := floats{
			AvgRating: y,
			StartYear: x,
		}

		cluster := clustermap[f]
		return colorLookup[cluster]
	}

	graph := chart.Chart{
		Series: []chart.Series{
			chart.ContinuousSeries{
				Style: chart.Style{
					StrokeWidth:      chart.Disabled,
					DotWidth:         5,
					DotColorProvider: viridisByY,
				},
				XValues: xValues,
				YValues: yValues,
			},
		},
	}

	return graph
}

func main() {
	start := time.Now()

	//minMaxes := getMinAndMax()
	//addKmeansNormalized(minMaxes)
	//getKDocumentsFromGenre(10, "Action")
	//oneStepKMeans("Action")
	runKMeansOnGenresAndSizes()

	//visualizeCluster("Action")

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
