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
	Id              int    `bson:"_id"`
	KmeansStartYear string `bson:"kmeansStartYear"`
	KmeansAvgRating string `bson:"kmeansAvgRating"`
}

type clusters struct {
	Clusters []cluster
}

type kmeans struct {
	KmeansStartYear decimal.Decimal
	KmeansAvgRating decimal.Decimal
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
	sampleKRandomDocs := bson.D{{"$sample", bson.D{{"size", k}}}}

	cursor, err := client.Database("assignment_eight").Collection("Movies").Aggregate(context.Background(),
		mongo.Pipeline{filterForMoviesStage, filterOutNoVotes, filterOutNoRating, filterOutTooLittleVotes,
			unwindGenresStage, filterForGenre, sampleKRandomDocs})

	if err != nil {
		log.Fatal(err)
	}

	defer cursor.Close(context.Background())

	clusterID := 1

	var operations []mongo.WriteModel

	for cursor.Next(context.Background()) {

		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)
		if err != nil {
			log.Fatal(err)
		}

		var c cluster
		c.Id = clusterID
		c.KmeansStartYear = jsonMap["kmeansNorm"].([]interface{})[0].(string)
		c.KmeansAvgRating = jsonMap["kmeansNorm"].([]interface{})[1].(string)

		operationA := mongo.NewInsertOneModel()
		operationA.SetDocument(c)

		operations = append(operations, operationA)

		clusterID += 1
	}

	_, err = client.Database("assignment_eight").Collection("centroids").
		BulkWrite(context.TODO(), operations)
	if err != nil {
		log.Fatal(err)
	}

}

func oneStepKMeans(g string) {

	client := connectToMongo()

	cursor, err := client.Database("assignment_eight").Collection("centroids").Find(context.Background(), bson.D{})

	if err != nil {
		log.Error(err)
	}

	var clusterList clusters

	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var c cluster
		err = bson.UnmarshalExtJSON([]byte(cursor.Current.String()), false, &c)

		if err != nil {
			log.Error(err)
		}

		clusterList.Clusters = append(clusterList.Clusters, c)
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

	clusterMap := make(map[int][]kmeans)

	for cursor.Next(context.Background()) {

		jsonMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(cursor.Current.String()), &jsonMap)

		if err != nil {
			log.Error(err)
		}

		kmeansStartYear, err := decimal.NewFromString(jsonMap["kmeansNorm"].([]interface{})[0].(string))

		if err != nil {
			log.Error(err)
		}

		kmeansAvgRating, err := decimal.NewFromString(jsonMap["kmeansNorm"].([]interface{})[1].(string))
		if err != nil {
			log.Error(err)
		}

		closestClusterID := 0
		closestClusterDistance := math.MaxFloat64

		for _, cluster := range clusterList.Clusters {
			if cluster.KmeansAvgRating == "" || cluster.KmeansStartYear == "" {
				continue
			}

			clusterKmeansAvgRating, err := decimal.NewFromString(cluster.KmeansAvgRating)
			if err != nil {
				log.Error(err)
			}

			clusterKmeansStartYear, err := decimal.NewFromString(cluster.KmeansStartYear)
			if err != nil {
				log.Error(err)
			}

			diffFloat, _ := (clusterKmeansAvgRating.Sub(kmeansAvgRating).Pow(decimal.NewFromInt(2))).
				Add(clusterKmeansStartYear.Sub(kmeansStartYear).Pow(decimal.NewFromInt(2))).BigFloat().Float64()
			distance := math.Sqrt(diffFloat)

			if distance < closestClusterDistance {
				closestClusterID = cluster.Id
				closestClusterDistance = distance
			}
		}

		clusterMap[closestClusterID] = append(clusterMap[closestClusterID], kmeans{
			KmeansStartYear: kmeansStartYear,
			KmeansAvgRating: kmeansAvgRating,
		})

		id, err := strconv.Atoi(jsonMap["_id"].(map[string]interface{})["$numberInt"].(string))
		if err != nil {
			log.Error(err)
		}

		_, err = client.Database("assignment_eight").Collection("Movies").
			UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"cluster": closestClusterID}})

	}

	for id, kmeanList := range clusterMap {

		avgRating := decimal.NewFromInt(0)
		startYear := decimal.NewFromInt(0)

		numMeans := 0

		for _, kmean := range kmeanList {

			avgRating = avgRating.Add(kmean.KmeansAvgRating)
			startYear = startYear.Add(kmean.KmeansStartYear)

			numMeans += 1
		}

		_, err = client.Database("assignment_eight").Collection("centroids").
			UpdateOne(context.Background(), bson.M{"_id": id},
				bson.M{"$set": bson.M{"kmeansStartYear": startYear.Div(decimal.NewFromInt(int64(numMeans))).String(),
					"kmeansAvgRating": avgRating.Div(decimal.NewFromInt(int64(numMeans))).String()}})

		if err != nil {
			log.Error(err)
		}

	}
}

func getSumOfSquaredErrors(g string) float64 {

	client := connectToMongo()

	cursor, err := client.Database("assignment_eight").Collection("centroids").Find(context.Background(), bson.D{})

	if err != nil {
		log.Error(err)
	}

	clustermap := make(map[int]cluster)

	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {

		var c cluster
		err = bson.UnmarshalExtJSON([]byte(cursor.Current.String()), false, &c)

		if err != nil {
			log.Error(err)
		}

		clustermap[c.Id] = c
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

		kmeansStartYear, err := decimal.NewFromString(jsonMap["kmeansNorm"].([]interface{})[0].(string))

		if err != nil {
			log.Error(err)
		}

		kmeansAvgRating, err := decimal.NewFromString(jsonMap["kmeansNorm"].([]interface{})[1].(string))
		if err != nil {
			log.Error(err)
		}

		clusterId, err := strconv.Atoi(jsonMap["cluster"].(map[string]interface{})["$numberInt"].(string))
		if err != nil {
			log.Error(err)
		}

		cluster := clustermap[clusterId]

		clusterKmeansAvgRating, err := decimal.NewFromString(cluster.KmeansAvgRating)
		if err != nil {
			log.Error(err)
		}

		clusterKmeansStartYear, err := decimal.NewFromString(cluster.KmeansAvgRating)
		if err != nil {
			log.Error(err)
		}

		avgRatingError, _ := kmeansAvgRating.Sub(clusterKmeansAvgRating).Pow(decimal.NewFromInt(2)).BigFloat().Float64()
		startYear, _ := kmeansStartYear.Sub(clusterKmeansStartYear).Pow(decimal.NewFromInt(2)).BigFloat().Float64()

		sse += avgRatingError
		sse += startYear

	}

	return sse
}

func runKMeansOnGenresAndSizes() {

	//genres := []string{"Action", "Horror", "Romance", "Sci-Fi", "Thriller"}
	genres := []string{"Action"}

	for _, genre := range genres {

		var data []float64
		var clusterSizes []float64

		for k := 10; k <= 50; k += 5 {
			getKDocumentsFromGenre(k, genre)

			for i := 0; i < 100; i++ {
				oneStepKMeans(genre)

				graph := visualizeCluster(genre)

				f, _ := os.Create("Eight/" + genre + "_" + strconv.Itoa(i) + ".png")

				err := graph.Render(chart.PNG, f)
				if err != nil {
					log.Error(err)
				}

				f.Close()
			}

			sse := getSumOfSquaredErrors(genre)
			data = append(data, sse)
			fmt.Println(sse)

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
	colorLookup[1] = drawing.ColorFromHex("#800000")
	colorLookup[2] = drawing.ColorFromHex("#9A6324")
	colorLookup[3] = drawing.ColorFromHex("#808000")
	colorLookup[4] = drawing.ColorFromHex("#469990")
	colorLookup[5] = drawing.ColorFromHex("#000075")
	colorLookup[6] = drawing.ColorFromHex("#000000")
	colorLookup[7] = drawing.ColorFromHex("#e6194B")
	colorLookup[8] = drawing.ColorFromHex("#f58231")
	colorLookup[9] = drawing.ColorFromHex("#ffe119")
	colorLookup[10] = drawing.ColorFromHex("#bfef45")
	colorLookup[11] = drawing.ColorFromHex("#3cb44b")
	colorLookup[12] = drawing.ColorFromHex("#42d4f4")
	colorLookup[13] = drawing.ColorFromHex("#4363d8")
	colorLookup[14] = drawing.ColorFromHex("#911eb4")
	colorLookup[15] = drawing.ColorFromHex("#f032e6")
	colorLookup[16] = drawing.ColorFromHex("#a9a9a9")
	colorLookup[17] = drawing.ColorFromHex("#fabed4")
	colorLookup[18] = drawing.ColorFromHex("#ffd8b1")
	colorLookup[19] = drawing.ColorFromHex("#fffac8")
	colorLookup[20] = drawing.ColorFromHex("#aaffc3")
	colorLookup[21] = drawing.ColorFromHex("#dcbeff")

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
	//getKDocumentsFromGenre(100, "Action")
	//oneStepKMeans("Action")
	runKMeansOnGenresAndSizes()

	//visualizeCluster("Action")

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
