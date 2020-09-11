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

type titles struct {
	Titles []title
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

func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	getTitlesFromLink(conn)
}
