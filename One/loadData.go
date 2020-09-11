package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"strconv"
	"strings"
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

func getTitlesFromLink(conn *pgx.Conn) map[string]int {

	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.basics.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")
			fmt.Println(row)

			m[row[0]] = idx

			isAdult, err := strconv.ParseBool(row[4])
			if err != nil {
				log.Fatal(err)
			}

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

	return m
}

func getEpisodesFromLink(conn *pgx.Conn, m map[string]int) {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.episode.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")
			fmt.Println(row)

			titleID := m[row[0]]
			seasonTitleID := m[row[1]]

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

func getPeopleFromLink(conn *pgx.Conn) map[string]int {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\name.basics.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")
			fmt.Println(row)

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
	return m
}

func getPrincipalsFromLink(conn *pgx.Conn, titleMap map[string]int, peopleMap map[string]int) {
	data, err := ioutil.ReadFile("C:\\Users\\Dan\\Documents\\College\\Intro to Big Data\\Assignments\\One\\title.principals.tsv\\data.tsv")
	if err != nil {
		log.Fatal(err)
	}

	uncompressedString := string(data)

	m := make(map[string]int)

	for idx, elem := range strings.Split(uncompressedString, "\n") {
		if idx != 0 {
			row := strings.Split(elem, "\t")
			fmt.Println(row)

			m[row[0]] = idx

			var ordering int
			if row[1] != "\\N" {
				ordering, err = strconv.Atoi(row[1])
				if err != nil {
					log.Fatal(err)
				}
			}

			p := principal{
				TitleID:  titleMap[row[0]],
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

func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	//titleMap := getTitlesFromLink(conn)
	//
	//getEpisodesFromLink(conn, titleMap)
	//peopleMap := getPeopleFromLink(conn)
	getPrincipalsFromLink(conn, make(map[string]int), make(map[string]int))
}
