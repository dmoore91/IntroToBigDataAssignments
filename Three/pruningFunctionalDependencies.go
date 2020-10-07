package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

type movieTitleActor struct {
	TitleID   int
	TitleType sql.NullString
	StartYear sql.NullInt32
	Runtime   int
	AvgRating decimal.NullDecimal
	GenreId   int
	Genre     sql.NullString
	MemberId  int
	BirthYear sql.NullInt32
	Role      string
}

type titleIdMaps struct {
	TitleType map[int]sql.NullString
	StartYear map[int]sql.NullInt32
	Runtime   map[int]int
	AvgRating map[int]decimal.NullDecimal
	GenreId   map[int]int
	Genre     map[int]sql.NullString
	MemberId  map[int]int
	BirthYear map[int]sql.NullInt32
	Role      map[int]string
}

func readInData() []movieTitleActor {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_three")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT movieID , type, startYear, runtimeMinutes, avgRating, genre_id, genre, member_id, " +
		"birthYear, role " +
		"FROM Movie_Actor_Role"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	var data []movieTitleActor

	defer rows.Close()

	defer rows.Close()

	for rows.Next() {
		var m = movieTitleActor{}
		err = rows.Scan(&m.TitleID, &m.TitleType, &m.StartYear, &m.Runtime, &m.AvgRating, &m.GenreId, &m.Genre,
			&m.MemberId, &m.BirthYear, &m.Role)

		if err != nil {
			log.Error(err)
		}

		data = append(data, m)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func checkMovieID(wg *sync.WaitGroup, data []movieTitleActor) {

	defer wg.Done()

	maps := titleIdMaps{
		TitleType: make(map[int]sql.NullString),
		StartYear: make(map[int]sql.NullInt32),
		Runtime:   make(map[int]int),
		AvgRating: make(map[int]decimal.NullDecimal),
		GenreId:   make(map[int]int),
		Genre:     make(map[int]sql.NullString),
		MemberId:  make(map[int]int),
		BirthYear: make(map[int]sql.NullInt32),
		Role:      make(map[int]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// type
		titleType, ok := maps.TitleType[elem.TitleID]

		if !ok {
			maps.TitleType[elem.TitleID] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {

				isValid[0] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.TitleID]

		if !ok {
			maps.StartYear[elem.TitleID] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear.Int32 != startYear.Int32 {
				isValid[1] = false
			}
		}

		// runtimeMinutes
		runtimeMinutes, ok := maps.Runtime[elem.TitleID]

		if !ok {
			maps.Runtime[elem.TitleID] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime != runtimeMinutes {
				isValid[2] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.TitleID]

		if !ok {
			maps.AvgRating[elem.TitleID] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
				isValid[3] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.TitleID]

		if !ok {
			maps.GenreId[elem.TitleID] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[4] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.TitleID]

		if !ok {
			maps.Genre[elem.TitleID] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre.String != genre.String {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.TitleID]

		if !ok {
			maps.MemberId[elem.TitleID] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.TitleID]

		if !ok {
			maps.BirthYear[elem.TitleID] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.TitleID]

		if !ok {
			maps.Role[elem.TitleID] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "movieID->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "type")
			case 1:
				println(header + "startYear")
			case 2:
				println(header + "runtimeMinutes")
			case 3:
				println(header + "avgRating")
			case 4:
				println(header + "genre_id")
			case 5:
				println(header + "genre")
			case 6:
				println(header + "member_id")
			case 7:
				println(header + "birthYear")
			case 8:
				println(header + "role")
			}
		}
	}
}

func main() {
	start := time.Now()

	data := readInData()

	// Since we have to brute force, I will spin up 10 go routines. 1 for each column
	// I will then iterate through the list of data, keep track of the mappings and if they change, I will
	// store that that dependency doesn't match in a 10 element boolean array.
	// Once that's done, I will print out text to tell us the dependencies

	wg := new(sync.WaitGroup)

	wg.Add(1)

	go checkMovieID(wg, data)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
