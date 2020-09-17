// Daniel Moore
// 9/10/2020
// This code loads the data sequentially using one connection.
// It takes roughly 6hrs and 45 minutes on my machine
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"strconv"
	"strings"
	"time"
)

type title struct {
	TitleID        int
	TitleType      string
	PrimaryTitle   string
	OriginalTitle  string
	IsAdult        bool
	StartYear      int
	EndYear        int
	RuntimeMinutes int
	Genres         []string
}

type episode struct {
	TitleID       int
	SeriesTitleID int
	SeasonNumber  int
	EpisodeNumber int
}

type people struct {
	PeopleID    int
	PrimaryName string
	BirthYear   int
	DeathYear   int
}

type principal struct {
	TitleID  int
	Ordering int
	PeopleID int
	Category string
}

type ratings struct {
	TitleID       int
	AverageRating float64
	NumVotes      int
}

func getTitlesFromLinkSlow(conn *pgx.Conn) map[string]int {

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			if len(row) == 9 {

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

					t := title{
						TitleID:        idx,
						TitleType:      row[1],
						PrimaryTitle:   row[2],
						OriginalTitle:  row[3],
						IsAdult:        isAdult,
						StartYear:      startYear,
						EndYear:        endYear,
						RuntimeMinutes: runtimeMinutes,
						Genres:         genres,
					}

					queryString := "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
						"runtimeMinutes,genres) " +
						"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

					commandTag, err := conn.Exec(context.Background(), queryString, t.TitleID, t.TitleType, t.PrimaryTitle, t.OriginalTitle,
						t.IsAdult, t.StartYear, t.EndYear, t.RuntimeMinutes, t.Genres)

					if err != nil {
						log.Fatal(err)
					}

					if commandTag.RowsAffected() == 0 {
						log.Fatal(err)
					}
				}
			}
		}
	}

	return m
}

func getEpisodesFromLinkSlow(conn *pgx.Conn, m map[string]int) {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\episode.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 && len(elem) == 4 {
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

				e := episode{
					TitleID:       titleID,
					SeriesTitleID: seasonTitleID,
					SeasonNumber:  seasonNumber,
					EpisodeNumber: episodeNumber,
				}

				queryString := "INSERT INTO episode(titleID, seriesTitleID, seasonNumber, episodeNumber) " +
					"VALUES ($1, $2, $3, $4)"

				commandTag, err := conn.Exec(context.Background(), queryString, e.TitleID, e.SeriesTitleID, e.SeasonNumber,
					e.EpisodeNumber)

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}
			}
		}
	}
}

func getPeopleFromLinkSlow(conn *pgx.Conn) map[string]int {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\name.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			if len(row) == 4 {

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

				p := people{
					PeopleID:    idx,
					PrimaryName: row[1],
					BirthYear:   birthYear,
					DeathYear:   deathYear,
				}

				queryString := "INSERT INTO people(peopleID, primaryName, birthYear, deathYear) " +
					"VALUES ($1, $2, $3, $4)"

				commandTag, err := conn.Exec(context.Background(), queryString, p.PeopleID, p.PrimaryName, p.BirthYear,
					p.DeathYear)

				if err != nil {
					log.Fatal(err)
				}

				if commandTag.RowsAffected() == 0 {
					log.Fatal(err)
				}
			}
		}
	}
	return m
}

func getPrincipalsFromLinkSlow(conn *pgx.Conn, titleMap map[string]int, peopleMap map[string]int) {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\principals.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			if len(row) == 4 {

				titleID, exists := titleMap[row[0]]

				if exists {

					var ordering int
					if row[1] != "\\N" {
						ordering, err = strconv.Atoi(row[1])
						if err != nil {
							log.Fatal(err)
						}
					}

					p := principal{
						TitleID:  titleID,
						Ordering: ordering,
						PeopleID: peopleMap[row[2]],
						Category: row[3],
					}

					queryString := "INSERT INTO principal(titleID, ordering, peopleID, category) " +
						"VALUES ($1, $2, $3, $4)"

					commandTag, err := conn.Exec(context.Background(), queryString, p.TitleID, p.Ordering, p.PeopleID,
						p.Category)

					if err != nil {
						log.Fatal(err)
					}

					if commandTag.RowsAffected() == 0 {
						log.Fatal(err)
					}
				}
			}
		}
	}
}

func addDirectorsSlow(conn *pgx.Conn, people []string, peopleMap map[string]int, crewID int) {
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
}

func addWritersSlow(conn *pgx.Conn, people []string, peopleMap map[string]int, crewID int) {
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
}

func getCrewFromLinkSlow(conn *pgx.Conn, titleMap map[string]int, peopleMap map[string]int) {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\crew.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			if len(row) == 3 {

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

					addDirectorsSlow(conn, strings.Split(row[1], ","), peopleMap, idx) // Method to add directors to linking table
					addWritersSlow(conn, strings.Split(row[2], ","), peopleMap, idx)   // Method to add writers to linking table
				}
			}
		}
	}
}

func getRatingsFromLinkSlow(conn *pgx.Conn, titleMap map[string]int) {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\ratings.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")

			if len(row) == 3 {

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

					r := ratings{
						TitleID:       titleID,
						AverageRating: averageRating,
						NumVotes:      numVotes,
					}

					queryString := "INSERT INTO ratings(titleID, averageRating, numVotes) " +
						"VALUES ($1, $2, $3)"

					commandTag, err := conn.Exec(context.Background(), queryString, r.TitleID, r.AverageRating, r.NumVotes)

					if err != nil {
						log.Fatal(err)
					}

					if commandTag.RowsAffected() == 0 {
						log.Fatal(err)
					}
				}
			}
		}
	}
}

func main() {

	start := time.Now()

	//Connect to db
	// All code shares on connection
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	//Load titles and create a map of tconst to primary_key
	titleMap := getTitlesFromLinkSlow(conn)
	fmt.Println("Finished getting titles")

	//Load people and create a map of nconst to primary_key
	peopleMap := getPeopleFromLinkSlow(conn)
	fmt.Println("Finished getting peoples")

	//Load episodes using title map
	getEpisodesFromLinkSlow(conn, titleMap)
	fmt.Println("Finished getting episodes")

	//Load principals using title and people map
	getPrincipalsFromLinkSlow(conn, titleMap, peopleMap)
	fmt.Println("Finished getting principals")

	//Load crews using title and people map
	getCrewFromLinkSlow(conn, titleMap, peopleMap)
	fmt.Println("Finished getting crew")

	//Load ratings using title map
	getRatingsFromLinkSlow(conn, titleMap)
	fmt.Println("Finished getting ratings")

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
