package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/jackc/pgx"
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
func readInRatings(m map[string]title) map[string]title {

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/ratings.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		txt := scanner.Text()

		i := strings.Index(txt, "\\N")

		for {
			if i == -1 {
				break
			}

			txt = txt[:i] + txt[i+2:]
			i = strings.Index(txt, "\\N")
		}

		row := strings.Split(txt, "\t")
		if len(row) == 3 {

			t := m[row[0]]
			t.AvgRating = row[1]
			t.NumVotes = row[2]

			m[row[0]] = t
		}
	}

	return m
}

func processGenres(genres map[string]int, genreList []string, titleID string, w *csv.Writer) {

	genreNumber := 0

	for _, elem := range genreList {
		genreID, ok := genres[elem]
		if ok {
			genres[elem] = genreNumber
			genreID = genreNumber
			genreNumber += 1
		}

		var line []string

		line = append(line, titleID)
		line = append(line, strconv.Itoa(genreID))

		err := w.Write(line)
		if err != nil {
			log.Fatal(err)
		}
	}
}

//Reads titles file into graviton db
func readInTitles(m map[string]title) map[string]title {

	genres := make(map[string]int)

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/title.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	idx := 0

	file, err = os.Create("Two/genre.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)

	for scanner.Scan() {
		txt := scanner.Text()

		if !strings.Contains(txt, "startyear") {
			i := strings.Index(txt, "\\N")

			for {
				if i == -1 {
					break
				}

				txt = txt[:i] + txt[i+2:]
				i = strings.Index(txt, "\\N")
			}

			row := strings.Split(txt, "\t")
			if len(row) == 9 {

				id := strconv.Itoa(idx)

				t := title{
					Id:             id,
					TitleType:      row[1],
					OriginalTitle:  row[3],
					StartYear:      row[5],
					EndYear:        row[6],
					RuntimeMinutes: row[7],
				}

				m[row[0]] = t

				idx += 1

				processGenres(genres, strings.Split(row[8], ","), t.Id, w)
			}
		}
	}

	return m
}

//Iterates through all elements in db and
func addElementsToDb(m map[string]title) {

	file, err := os.Create("Two/result.csv")
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

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title FROM '/home/danielmoore/Documents/College/BigData/Two/result.csv' DELIMITER ',' CSV;"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}

	queryString = "COPY Title FROM '/home/danielmoore/Documents/College/BigData/Two/genre.csv' DELIMITER ',' CSV;"

	commandTag, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}
}

func populateTitleTable(wg *sync.WaitGroup) {

	defer wg.Done()

	titles := make(map[string]title)

	titles = readInTitles(titles)
	titles = readInRatings(titles)
	addElementsToDb(titles)
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
