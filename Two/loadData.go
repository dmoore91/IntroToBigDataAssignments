package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/deroproject/graviton"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type title struct {
	Id             string
	TitleType      string
	OriginalTitle  string
	StartYear      string
	EndYear        string
	RuntimeMinutes string
	AvgRating      string
	NumVotes       string
}

func (t title) ToSlice() []string {
	var items []string

	items = append(items, t.Id)
	items = append(items, t.TitleType)
	items = append(items, t.OriginalTitle)
	items = append(items, t.StartYear)
	items = append(items, t.EndYear)
	items = append(items, t.RuntimeMinutes)
	items = append(items, t.AvgRating)
	items = append(items, t.NumVotes)

	return items
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

			t := getKey(tree, row[0])
			t.AvgRating = row[1]
			t.NumVotes = row[2]

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

			id := strconv.Itoa(idx)

			t := getKey(tree, row[0])
			t.Id = id
			t.TitleType = row[1]
			t.OriginalTitle = row[3]
			t.StartYear = row[5]
			t.EndYear = row[6]
			t.RuntimeMinutes = row[7]

			updateKey(tree, row[0], t)

			//TODO Kick off goroutine to add genres to table. And link table with genres
		}
	}
}

//Iterates through all elements in db and
func addElementsToDb(tree *graviton.Tree, wg *sync.WaitGroup) {

	defer wg.Done()

	file, err := os.Create("result.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)

	c := tree.Cursor()

	for k, v, err := c.First(); err == nil; k, v, err = c.Next() {
		_ = k //Needed to make Go think I'm using k. Literally just assigns k to nothing
		r := bytes.NewReader(v)

		var t title

		err = json.NewDecoder(r).Decode(t)
		if err != nil {
			log.Fatal(err)
		}

		if err := w.Write(t.ToSlice()); err != nil {
			log.Fatal()
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
