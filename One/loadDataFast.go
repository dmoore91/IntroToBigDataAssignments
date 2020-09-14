// Daniel Moore
// 9/13/2020
// This code loads the data in parallel. We first kick off 2 goroutines with their own connections to load in titles
// and people. These also create the string to primary_key maps we need. Next we kick of the 4 goroutines to load
// in crew, ratings, episodes and principals. These all have their own connections and run fully in parallel. The reason
// they all have their own connections to the db is to prevent errors from code trying to use an occupied connection.
// I would have liked to have addDirectors and addWriters in their own goroutines with their own connections as well
// instead of holding up importing crews, however I only have a max of 400 database connections and the risk of
// exceeding the max connections is too high.
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"
)

func getTitlesFromLink(titleMap *map[string]int, wg *sync.WaitGroup) {
	fmt.Println("Start getting titles")

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.basics.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	count := 0

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			isAdult, err := strconv.ParseBool(row[4])
			if err != nil {
				log.Fatal(err)
			}

			if !isAdult {

				m[row[0]] = idx

				var startYear int
				if row[5] != "\\N" {
					startYear, err = strconv.Atoi(row[5])
					if err != nil {
						log.Fatal(err)
					}
				}

				var endYear int
				if row[6] != "\\N" {
					endYear, err = strconv.Atoi(row[6])
					if err != nil {
						log.Fatal(err)
					}
				}

				var runtimeMinutes int

				if row[7] != "\\N" {
					runtimeMinutes, err = strconv.Atoi(row[7])
					if err != nil {
						log.Fatal(err)
					}
				}

				var genres []string

				if row[8] != "\\N" {
					genres = strings.Split(row[8], ",")
					if err != nil {
						log.Fatal(err)
					}
				}

				queryString := "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
					"runtimeMinutes,genres) " +
					"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

				commandTag, err := conn.Exec(context.Background(), queryString, idx, row[1], row[2], row[3],
					isAdult, startYear, endYear, runtimeMinutes, genres)

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}
			}
		}

		if count > 5 {
			break
		}

		count += 1
	}

	fmt.Println("Finished getting titles")

	titleMap = &m
	fmt.Println(m)

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Close conn")

	wg.Done()

}

func getEpisodesFromLink(m map[string]int, wg *sync.WaitGroup) {
	fmt.Println("Start getting episodes")

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	defer wg.Done()

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.episode.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")
			fmt.Println(row)

			titleID, episodeMatch := m[row[0]]
			seasonTitleID, seasonMatch := m[row[1]]

			if episodeMatch && seasonMatch {
				var seasonNumber int
				if row[2] != "\\N" {
					seasonNumber, err = strconv.Atoi(row[2])
					if err != nil {
						log.Fatal(err)
					}
				}

				var episodeNumber int
				if row[3] != "\\N" {
					episodeNumber, err = strconv.Atoi(row[3])
					if err != nil {
						log.Fatal(err)
					}
				}

				queryString := "INSERT INTO episode(titleID, seriesTitleID, seasonNumber, episodeNumber) " +
					"VALUES ($1, $2, $3, $4)"

				commandTag, err := conn.Exec(context.Background(), queryString, titleID, seasonTitleID, seasonNumber,
					episodeNumber)

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}
			}
		}
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Finished getting episodes")
}

func getPeopleFromLink(peopleMap *map[string]int, wg *sync.WaitGroup) {
	fmt.Println("Start getting people")

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\name.basics.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	count := 0

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			m[row[0]] = idx

			var birthYear int
			if row[2] != "\\N" {
				birthYear, err = strconv.Atoi(row[2])
				if err != nil {
					log.Fatal(err)
				}
			}

			var deathYear int
			if row[3] != "\\N" {
				deathYear, err = strconv.Atoi(row[3])
				if err != nil {
					log.Fatal(err)
				}
			}

			queryString := "INSERT INTO people(peopleID, primaryName, birthYear, deathYear) " +
				"VALUES ($1, $2, $3, $4)"

			commandTag, err := conn.Exec(context.Background(), queryString, idx, row[1], birthYear, deathYear)

			if err != nil {
				log.Fatal(err)
			}

			if commandTag.RowsAffected() == 0 {
				log.Fatal(err)
			}
		}

		if count > 5 {
			break
		}

		count += 1
	}

	fmt.Println("Finished getting peoples")

	peopleMap = &m
	fmt.Println(m)

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Close context")

	wg.Done()
}

func getPrincipalsFromLink(titleMap map[string]int, peopleMap map[string]int, wg *sync.WaitGroup) {
	fmt.Println("Start getting principals")

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	defer wg.Done()

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.principals.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			titleID, titleExists := titleMap[row[0]]
			peopleID, peopleExists := peopleMap[row[2]]

			if titleExists && peopleExists {

				var ordering int
				if row[1] != "\\N" {
					ordering, err = strconv.Atoi(row[1])
					if err != nil {
						log.Fatal(err)
					}
				}

				queryString := "INSERT INTO principal(titleID, ordering, peopleID, category) " +
					"VALUES ($1, $2, $3, $4)"

				commandTag, err := conn.Exec(context.Background(), queryString, titleID, ordering,
					peopleID, row[3])

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}
			}
		}
	}

	fmt.Println("Finished getting principals")
	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func addDirectors(conn *pgx.Conn, people []string, peopleMap map[string]int, crewID int) {

	for _, elem := range people {
		queryString := "INSERT INTO directors(crewID, peopleID) " +
			"VALUES ($1, $2)"

		commandTag, err := conn.Exec(context.Background(), queryString, crewID, peopleMap[elem])

		if err != nil {
			log.Fatal(err)
		}

		if commandTag.RowsAffected() == 0 {
			log.Fatal(err)
		}
	}

	err := conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func addWriters(conn *pgx.Conn, people []string, peopleMap map[string]int, crewID int) {

	for _, elem := range people {
		queryString := "INSERT INTO writers(crewID, peopleID) " +
			"VALUES ($1, $2)"

		commandTag, err := conn.Exec(context.Background(), queryString, crewID, peopleMap[elem])

		if err != nil {
			log.Fatal(err)
		}

		if commandTag.RowsAffected() == 0 {
			log.Fatal(err)
		}
	}

	err := conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func getCrewFromLink(titleMap map[string]int, peopleMap map[string]int, wg *sync.WaitGroup) {
	fmt.Println("Start getting crew")

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	defer wg.Done()

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.crew.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			titleID, exists := titleMap[row[0]]

			if exists {

				queryString := "INSERT INTO crew(titleID, crewID) " +
					"VALUES ($1, $2)"

				commandTag, err := conn.Exec(context.Background(), queryString, titleID, idx)

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}

				addDirectors(conn, strings.Split(row[1], ","), peopleMap, idx) // Method to add directors to linking table
				addWriters(conn, strings.Split(row[2], ","), peopleMap, idx)   // Method to add writers to linking table
			}
		}
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Finished getting crew")
}

func getRatingsFromLink(titleMap map[string]int, wg *sync.WaitGroup) {
	fmt.Println("Start getting ratings")

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	defer wg.Done()

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.ratings.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			titleID, exists := titleMap[row[0]]

			if exists {

				averageRating, err := strconv.ParseFloat(row[1], 32)
				if err != nil {
					log.Fatal(err)
				}

				var numVotes int
				if row[2] != "\\N" {
					numVotes, err = strconv.Atoi(row[2])
					if err != nil {
						log.Fatal(err)
					}
				}

				queryString := "INSERT INTO ratings(titleID, averageRating, numVotes) " +
					"VALUES ($1, $2, $3)"

				commandTag, err := conn.Exec(context.Background(), queryString, titleID, averageRating, numVotes)

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}
			}
		}
	}

	fmt.Println("Finished getting ratings")
	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	start := time.Now()

	var titleMap map[string]int
	var peopleMap map[string]int

	var wg sync.WaitGroup

	// Kick off goroutines to load titles and people. These also create the maps our other functions will need to run.
	wg.Add(2)
	go getTitlesFromLink(&titleMap, &wg)
	go getPeopleFromLink(&peopleMap, &wg)

	fmt.Println(titleMap)
	fmt.Println(peopleMap)

	// Wait for previous 2 go routines so we know we have the maps we need
	wg.Wait()

	// Kick off goroutines to load episodes, crew, principals and ratings.
	// These use the maps the previous goroutines created
	wg.Add(4)
	go getEpisodesFromLink(titleMap, &wg)
	go getPrincipalsFromLink(titleMap, peopleMap, &wg)
	go getCrewFromLink(titleMap, peopleMap, &wg)
	go getRatingsFromLink(titleMap, &wg)

	// Wait on all the goroutines to make sure we get the proper total time for loading in all the data
	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
