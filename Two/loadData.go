package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/deroproject/graviton"
	"github.com/jackc/pgx"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"
)

type title struct {
	Id             int
	TitleType      string
	OriginalTitle  string
	StartYear      int
	EndYear        int
	RuntimeMinutes int
	AvgRating      decimal.Decimal
	NumVotes       int
}

//This function is used to put t into the tconst location in the Badger database. It just overwrites
//whatever is already there
//i.e. {tconst: t}
func updateKey(tree *graviton.Tree, tconst string, t title) {

	reqBodyBytes := new(bytes.Buffer)
	err := json.NewEncoder(reqBodyBytes).Encode(t)
	if err != nil {
		log.Fatal(err)
	}

	err = tree.Put([]byte(tconst), reqBodyBytes.Bytes())
	if err != nil {
		log.Fatal(err)
	}
}

//This function is takes a tconst and returns the corresponding struct
//i.e. {tconst: t}
func getKey(tree *graviton.Tree, tconst string) title {

	v, err := tree.Get([]byte(tconst))
	if err != nil {
		log.Fatal(err)
		return title{}
	}

	r := bytes.NewReader(v)

	var t title

	err = json.NewDecoder(r).Decode(t)
	if err != nil {
		log.Fatal(err)
		return title{}
	}

	return t
}

//Reads ratings file into graviton db
func readInRatings(tree *graviton.Tree, wg *sync.WaitGroup) {
	defer wg.Done()

	data, err := ioutil.ReadFile("title.ratings.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	lines := strings.Split(uncompressedString, "\n")
	numLines := len(lines)

	for idx, elem := range lines {
		if idx != 0 && idx != numLines-1 {
			row := strings.Split(elem, "\t")

			avgRating, err := decimal.NewFromString(row[1])
			if err != nil {
				log.Fatal(err)
			}

			numVotes, err := strconv.Atoi(row[2])
			if err != nil {
				log.Fatal(err)
			}

			t := getKey(tree, row[0])
			t.AvgRating = avgRating
			t.NumVotes = numVotes

			updateKey(tree, row[0], t)
		}
	}
}

//Reads titles file into graviton db
func readInTitles(tree *graviton.Tree, wg *sync.WaitGroup) {
	defer wg.Done()

	data, err := ioutil.ReadFile("title.basics.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	lines := strings.Split(uncompressedString, "\n")
	numLines := len(lines)

	for idx, elem := range lines {
		if idx != 0 && idx != numLines-1 {
			row := strings.Split(elem, "\t")

			startYear, err := strconv.Atoi(row[5])
			if err != nil {
				log.Fatal(err)
			}

			endYear, err := strconv.Atoi(row[6])
			if err != nil {
				log.Fatal(err)
			}

			runtimeMinutes, err := strconv.Atoi(row[7])
			if err != nil {
				log.Fatal(err)
			}

			t := getKey(tree, row[0])
			t.Id = idx
			t.TitleType = row[1]
			t.OriginalTitle = row[3]
			t.StartYear = startYear
			t.EndYear = endYear
			t.RuntimeMinutes = runtimeMinutes

			updateKey(tree, row[0], t)

			//TODO Kick off goroutine to add genres to table. And link table with genres
		}
	}
}

//Iterates through all elements in db and
func addElementsToDb(tree *graviton.Tree, wg *sync.WaitGroup) {

	defer wg.Done()

	//Establish connection to postgres db
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	c := tree.Cursor()

	for k, v, err := c.First(); err == nil; k, v, err = c.Next() {
		_ = k //Needed to make Go think I'm using k. Literally just assigns k to nothing
		r := bytes.NewReader(v)

		var t title

		err = json.NewDecoder(r).Decode(t)
		if err != nil {
			log.Fatal(err)
		}

		queryString := "INSERT INTO Title(id, type, originalTitle, startYear, endYear, runtimeMinutes, " +
			"avgRating, numVotes) " +
			"VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

		commandTag, err := conn.Exec(context.Background(), queryString, t.Id, t.TitleType, t.OriginalTitle, t.StartYear,
			t.EndYear, t.RuntimeMinutes, t.AvgRating, t.NumVotes)

		if err != nil {
			log.Fatal(err)
		}

		if commandTag.RowsAffected() == 0 {
			log.Fatal(err)
		}
	}
}

func populateTitleTable(wg *sync.WaitGroup) {

	defer wg.Done()

	//Key-Value database that allows for very fast full tree traversal which is going to be very important
	store, err := graviton.NewMemStore()
	if err != nil {
		log.Fatal(err)
	}

	ss, err := store.LoadSnapshot(0) // load most recent snapshot
	if err != nil {
		log.Fatal(err)
	}

	tree, err := ss.GetTree("root")
	if err != nil {
		log.Fatal(err)
	}

	//Internal waitgroup for title related threads
	var titleWaitgroup sync.WaitGroup

	titleWaitgroup.Add(2)
	go readInRatings(tree, &titleWaitgroup)
	go readInTitles(tree, &titleWaitgroup)

	titleWaitgroup.Wait()

	titleWaitgroup.Add(1)
	go addElementsToDb(tree, &titleWaitgroup)

	titleWaitgroup.Wait() //Wait to finish adding all elements
}

func main() {

	start := time.Now()

	var wg sync.WaitGroup

	wg.Add(1)
	go populateTitleTable(&wg)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
