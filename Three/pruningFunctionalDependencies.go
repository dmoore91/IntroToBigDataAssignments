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

type typeMaps struct {
	TitleID   map[sql.NullString]int
	StartYear map[sql.NullString]sql.NullInt32
	Runtime   map[sql.NullString]int
	AvgRating map[sql.NullString]decimal.NullDecimal
	GenreId   map[sql.NullString]int
	Genre     map[sql.NullString]sql.NullString
	MemberId  map[sql.NullString]int
	BirthYear map[sql.NullString]sql.NullInt32
	Role      map[sql.NullString]string
}

type startYearMaps struct {
	TitleID   map[sql.NullInt32]int
	TitleType map[sql.NullInt32]sql.NullString
	Runtime   map[sql.NullInt32]int
	AvgRating map[sql.NullInt32]decimal.NullDecimal
	GenreId   map[sql.NullInt32]int
	Genre     map[sql.NullInt32]sql.NullString
	MemberId  map[sql.NullInt32]int
	BirthYear map[sql.NullInt32]sql.NullInt32
	Role      map[sql.NullInt32]string
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

func checkType(wg *sync.WaitGroup, data []movieTitleActor) {

	defer wg.Done()

	maps := typeMaps{
		TitleID:   make(map[sql.NullString]int),
		StartYear: make(map[sql.NullString]sql.NullInt32),
		Runtime:   make(map[sql.NullString]int),
		AvgRating: make(map[sql.NullString]decimal.NullDecimal),
		GenreId:   make(map[sql.NullString]int),
		Genre:     make(map[sql.NullString]sql.NullString),
		MemberId:  make(map[sql.NullString]int),
		BirthYear: make(map[sql.NullString]sql.NullInt32),
		Role:      make(map[sql.NullString]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.TitleType]

		if !ok {
			maps.TitleID[elem.TitleType] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.TitleType]

		if !ok {
			maps.StartYear[elem.TitleType] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear.Int32 != startYear.Int32 {
				isValid[1] = false
			}
		}

		// runtimeMinutes
		runtimeMinutes, ok := maps.Runtime[elem.TitleType]

		if !ok {
			maps.Runtime[elem.TitleType] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime != runtimeMinutes {
				isValid[2] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.TitleType]

		if !ok {
			maps.AvgRating[elem.TitleType] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
				isValid[3] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.TitleType]

		if !ok {
			maps.GenreId[elem.TitleType] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[4] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.TitleType]

		if !ok {
			maps.Genre[elem.TitleType] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre.String != genre.String {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.TitleType]

		if !ok {
			maps.MemberId[elem.TitleType] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.TitleType]

		if !ok {
			maps.BirthYear[elem.TitleType] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.TitleType]

		if !ok {
			maps.Role[elem.TitleType] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "type->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
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

func checkStartYear(wg *sync.WaitGroup, data []movieTitleActor) {

	defer wg.Done()

	maps := startYearMaps{
		TitleID:   make(map[sql.NullInt32]int),
		TitleType: make(map[sql.NullInt32]sql.NullString),
		Runtime:   make(map[sql.NullInt32]int),
		AvgRating: make(map[sql.NullInt32]decimal.NullDecimal),
		GenreId:   make(map[sql.NullInt32]int),
		Genre:     make(map[sql.NullInt32]sql.NullString),
		MemberId:  make(map[sql.NullInt32]int),
		BirthYear: make(map[sql.NullInt32]sql.NullInt32),
		Role:      make(map[sql.NullInt32]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.StartYear]

		if !ok {
			maps.TitleID[elem.StartYear] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.StartYear]

		if !ok {
			maps.TitleType[elem.StartYear] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {

				isValid[1] = false
			}
		}

		// runtimeMinutes
		runtimeMinutes, ok := maps.Runtime[elem.StartYear]

		if !ok {
			maps.Runtime[elem.StartYear] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime != runtimeMinutes {
				isValid[2] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.StartYear]

		if !ok {
			maps.AvgRating[elem.StartYear] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
				isValid[3] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.StartYear]

		if !ok {
			maps.GenreId[elem.StartYear] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[4] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.StartYear]

		if !ok {
			maps.Genre[elem.StartYear] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre.String != genre.String {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.StartYear]

		if !ok {
			maps.MemberId[elem.StartYear] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.StartYear]

		if !ok {
			maps.BirthYear[elem.StartYear] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.StartYear]

		if !ok {
			maps.Role[elem.StartYear] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "startYear->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
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

	// I will spin up 10 go routines. 1 for each column
	// I will then iterate through the list of data, keep track of the mappings and if they change, I will
	// store that that dependency doesn't match in a 9 element boolean array.
	// Once that's done, I will print out text to tell us the dependencies

	wg := new(sync.WaitGroup)

	wg.Add(3)

	go checkMovieID(wg, data)
	go checkType(wg, data)
	go checkStartYear(wg, data)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
