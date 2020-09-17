package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	log "github.com/sirupsen/logrus"
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

//Reads ratings file into graviton db
func readInRatings(m map[string]title, wg *sync.WaitGroup) {

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/ratings.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), "\t")
		if len(row) == 3 {

			t := m[row[0]]
			t.AvgRating = row[1]
			t.NumVotes = row[2]

			m[row[0]] = t
		}
	}

	wg.Done()
}

//Reads titles file into graviton db
func readInTitles(m map[string]title, wg *sync.WaitGroup) {

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/title.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	idx := 0

	for scanner.Scan() {
		row := strings.Split(scanner.Text(), "\t")
		if len(row) == 9 {
			print("test")

			id := strconv.Itoa(idx)

			t := m[row[0]]
			t.Id = id
			t.TitleType = row[1]
			t.OriginalTitle = row[3]
			t.StartYear = row[5]
			t.EndYear = row[6]
			t.RuntimeMinutes = row[7]

			m[row[0]] = t

			//TODO Kick off goroutine to add genres to table. And link table with genres
		}
	}

	wg.Done()
}

//Iterates through all elements in db and
func addElementsToDb(m map[string]title, wg *sync.WaitGroup) {

	file, err := os.Create("result.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)

	for k, t := range m {
		_ = k
		if err := w.Write(t.ToSlice()); err != nil {
			log.Fatal()
		}
	}
	wg.Done()
}

func populateTitleTable(wg *sync.WaitGroup) {

	defer wg.Done()

	titles := make(map[string]title)

	//Internal waitgroup for title related threads
	var titleWaitgroup sync.WaitGroup

	titleWaitgroup.Add(2)
	go readInTitles(titles, &titleWaitgroup)
	go readInRatings(titles, &titleWaitgroup)

	titleWaitgroup.Wait()

	titleWaitgroup.Add(1)
	go addElementsToDb(titles, &titleWaitgroup)

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
