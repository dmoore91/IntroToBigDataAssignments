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

type runtimeMap struct {
	TitleID   map[int]int
	TitleType map[int]sql.NullString
	StartYear map[int]sql.NullInt32
	AvgRating map[int]decimal.NullDecimal
	GenreId   map[int]int
	Genre     map[int]sql.NullString
	MemberId  map[int]int
	BirthYear map[int]sql.NullInt32
	Role      map[int]string
}

type avgRatingMap struct {
	TitleID   map[decimal.NullDecimal]int
	TitleType map[decimal.NullDecimal]sql.NullString
	StartYear map[decimal.NullDecimal]sql.NullInt32
	Runtime   map[decimal.NullDecimal]int
	GenreId   map[decimal.NullDecimal]int
	Genre     map[decimal.NullDecimal]sql.NullString
	MemberId  map[decimal.NullDecimal]int
	BirthYear map[decimal.NullDecimal]sql.NullInt32
	Role      map[decimal.NullDecimal]string
}

type genreIdMap struct {
	TitleID   map[int]int
	TitleType map[int]sql.NullString
	StartYear map[int]sql.NullInt32
	AvgRating map[int]decimal.NullDecimal
	Runtime   map[int]int
	Genre     map[int]sql.NullString
	MemberId  map[int]int
	BirthYear map[int]sql.NullInt32
	Role      map[int]string
}

type genreMap struct {
	TitleID   map[sql.NullString]int
	TitleType map[sql.NullString]sql.NullString
	StartYear map[sql.NullString]sql.NullInt32
	AvgRating map[sql.NullString]decimal.NullDecimal
	Runtime   map[sql.NullString]int
	GenreId   map[sql.NullString]int
	MemberId  map[sql.NullString]int
	BirthYear map[sql.NullString]sql.NullInt32
	Role      map[sql.NullString]string
}

type memberIdMap struct {
	TitleID   map[int]int
	TitleType map[int]sql.NullString
	StartYear map[int]sql.NullInt32
	AvgRating map[int]decimal.NullDecimal
	Runtime   map[int]int
	GenreId   map[int]int
	Genre     map[int]sql.NullString
	BirthYear map[int]sql.NullInt32
	Role      map[int]string
}

type birthYearMap struct {
	TitleID   map[sql.NullInt32]int
	TitleType map[sql.NullInt32]sql.NullString
	StartYear map[sql.NullInt32]sql.NullInt32
	AvgRating map[sql.NullInt32]decimal.NullDecimal
	Runtime   map[sql.NullInt32]int
	GenreId   map[sql.NullInt32]int
	Genre     map[sql.NullInt32]sql.NullString
	MemberId  map[sql.NullInt32]int
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

func checkRuntime(wg *sync.WaitGroup, data []movieTitleActor) {

	defer wg.Done()

	maps := runtimeMap{
		TitleID:   make(map[int]int),
		TitleType: make(map[int]sql.NullString),
		StartYear: make(map[int]sql.NullInt32),
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

		// titleID
		titleID, ok := maps.TitleID[elem.Runtime]

		if !ok {
			maps.TitleID[elem.Runtime] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.Runtime]

		if !ok {
			maps.TitleType[elem.Runtime] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {

				isValid[1] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.Runtime]

		if !ok {
			maps.StartYear[elem.Runtime] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear != startYear {
				isValid[2] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.Runtime]

		if !ok {
			maps.AvgRating[elem.Runtime] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
				isValid[3] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.Runtime]

		if !ok {
			maps.GenreId[elem.Runtime] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[4] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.Runtime]

		if !ok {
			maps.Genre[elem.Runtime] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre.String != genre.String {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.Runtime]

		if !ok {
			maps.MemberId[elem.Runtime] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.Runtime]

		if !ok {
			maps.BirthYear[elem.Runtime] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.Runtime]

		if !ok {
			maps.Role[elem.Runtime] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "runtime->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
			case 2:
				println(header + "startYear")
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

func checkAvgRating(wg *sync.WaitGroup, data []movieTitleActor) {

	defer wg.Done()

	maps := avgRatingMap{
		TitleID:   make(map[decimal.NullDecimal]int),
		TitleType: make(map[decimal.NullDecimal]sql.NullString),
		StartYear: make(map[decimal.NullDecimal]sql.NullInt32),
		Runtime:   make(map[decimal.NullDecimal]int),
		GenreId:   make(map[decimal.NullDecimal]int),
		Genre:     make(map[decimal.NullDecimal]sql.NullString),
		MemberId:  make(map[decimal.NullDecimal]int),
		BirthYear: make(map[decimal.NullDecimal]sql.NullInt32),
		Role:      make(map[decimal.NullDecimal]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.AvgRating]

		if !ok {
			maps.TitleID[elem.AvgRating] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.AvgRating]

		if !ok {
			maps.TitleType[elem.AvgRating] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {

				isValid[1] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.AvgRating]

		if !ok {
			maps.StartYear[elem.AvgRating] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear != startYear {
				isValid[2] = false
			}
		}

		// runtime
		runtime, ok := maps.Runtime[elem.AvgRating]

		if !ok {
			maps.Runtime[elem.AvgRating] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime == runtime {
				isValid[3] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.AvgRating]

		if !ok {
			maps.GenreId[elem.AvgRating] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[4] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.AvgRating]

		if !ok {
			maps.Genre[elem.AvgRating] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre.String != genre.String {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.AvgRating]

		if !ok {
			maps.MemberId[elem.AvgRating] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.AvgRating]

		if !ok {
			maps.BirthYear[elem.AvgRating] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.AvgRating]

		if !ok {
			maps.Role[elem.AvgRating] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "avgRating->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
			case 2:
				println(header + "startYear")
			case 3:
				println(header + "runtimeMinutes")
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

func checkGenreId(wg *sync.WaitGroup, data []movieTitleActor) {
	defer wg.Done()

	maps := genreIdMap{
		TitleID:   make(map[int]int),
		TitleType: make(map[int]sql.NullString),
		StartYear: make(map[int]sql.NullInt32),
		Runtime:   make(map[int]int),
		AvgRating: make(map[int]decimal.NullDecimal),
		Genre:     make(map[int]sql.NullString),
		MemberId:  make(map[int]int),
		BirthYear: make(map[int]sql.NullInt32),
		Role:      make(map[int]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.GenreId]

		if !ok {
			maps.TitleID[elem.GenreId] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.GenreId]

		if !ok {
			maps.TitleType[elem.GenreId] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {

				isValid[1] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.GenreId]

		if !ok {
			maps.StartYear[elem.GenreId] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear != startYear {
				isValid[2] = false
			}
		}

		// runtime
		runtime, ok := maps.Runtime[elem.GenreId]

		if !ok {
			maps.Runtime[elem.GenreId] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime == runtime {
				isValid[3] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.GenreId]

		if !ok {
			maps.AvgRating[elem.GenreId] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.AvgRating != avgRating {
				isValid[4] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.GenreId]

		if !ok {
			maps.Genre[elem.GenreId] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre.String != genre.String {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.GenreId]

		if !ok {
			maps.MemberId[elem.GenreId] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.GenreId]

		if !ok {
			maps.BirthYear[elem.GenreId] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.GenreId]

		if !ok {
			maps.Role[elem.GenreId] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "genre_id->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
			case 2:
				println(header + "startYear")
			case 3:
				println(header + "runtimeMinutes")
			case 4:
				println(header + "avgRating")
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

func checkGenre(wg *sync.WaitGroup, data []movieTitleActor) {
	defer wg.Done()

	maps := genreMap{
		TitleID:   make(map[sql.NullString]int),
		TitleType: make(map[sql.NullString]sql.NullString),
		StartYear: make(map[sql.NullString]sql.NullInt32),
		Runtime:   make(map[sql.NullString]int),
		AvgRating: make(map[sql.NullString]decimal.NullDecimal),
		GenreId:   make(map[sql.NullString]int),
		MemberId:  make(map[sql.NullString]int),
		BirthYear: make(map[sql.NullString]sql.NullInt32),
		Role:      make(map[sql.NullString]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.Genre]

		if !ok {
			maps.TitleID[elem.Genre] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.Genre]

		if !ok {
			maps.TitleType[elem.Genre] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {
				isValid[1] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.Genre]

		if !ok {
			maps.StartYear[elem.Genre] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear != startYear {
				isValid[2] = false
			}
		}

		// runtime
		runtime, ok := maps.Runtime[elem.Genre]

		if !ok {
			maps.Runtime[elem.Genre] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime == runtime {
				isValid[3] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.Genre]

		if !ok {
			maps.AvgRating[elem.Genre] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.AvgRating != avgRating {
				isValid[4] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.Genre]

		if !ok {
			maps.GenreId[elem.Genre] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[5] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.Genre]

		if !ok {
			maps.MemberId[elem.Genre] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.Genre]

		if !ok {
			maps.BirthYear[elem.Genre] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.Genre]

		if !ok {
			maps.Role[elem.Genre] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "genre->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
			case 2:
				println(header + "startYear")
			case 3:
				println(header + "runtimeMinutes")
			case 4:
				println(header + "avgRating")
			case 5:
				println(header + "genre_id")
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

func checkMemberID(wg *sync.WaitGroup, data []movieTitleActor) {
	defer wg.Done()

	maps := memberIdMap{
		TitleID:   make(map[int]int),
		TitleType: make(map[int]sql.NullString),
		StartYear: make(map[int]sql.NullInt32),
		Runtime:   make(map[int]int),
		AvgRating: make(map[int]decimal.NullDecimal),
		GenreId:   make(map[int]int),
		Genre:     make(map[int]sql.NullString),
		BirthYear: make(map[int]sql.NullInt32),
		Role:      make(map[int]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.MemberId]

		if !ok {
			maps.TitleID[elem.MemberId] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.MemberId]

		if !ok {
			maps.TitleType[elem.MemberId] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {
				isValid[1] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.MemberId]

		if !ok {
			maps.StartYear[elem.MemberId] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear != startYear {
				isValid[2] = false
			}
		}

		// runtime
		runtime, ok := maps.Runtime[elem.MemberId]

		if !ok {
			maps.Runtime[elem.MemberId] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime == runtime {
				isValid[3] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.MemberId]

		if !ok {
			maps.AvgRating[elem.MemberId] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.AvgRating != avgRating {
				isValid[4] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.MemberId]

		if !ok {
			maps.GenreId[elem.MemberId] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[5] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.MemberId]

		if !ok {
			maps.Genre[elem.MemberId] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre != genre {
				isValid[6] = false
			}
		}

		// birthYear
		birthYear, ok := maps.BirthYear[elem.MemberId]

		if !ok {
			maps.BirthYear[elem.MemberId] = elem.BirthYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.BirthYear.Int32 != birthYear.Int32 {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.MemberId]

		if !ok {
			maps.Role[elem.MemberId] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "member_id->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
			case 2:
				println(header + "startYear")
			case 3:
				println(header + "runtimeMinutes")
			case 4:
				println(header + "avgRating")
			case 5:
				println(header + "genre_id")
			case 6:
				println(header + "genre")
			case 7:
				println(header + "birthYear")
			case 8:
				println(header + "role")
			}
		}
	}
}

func checkBirthYear(wg *sync.WaitGroup, data []movieTitleActor) {

	defer wg.Done()

	maps := birthYearMap{
		TitleID:   make(map[sql.NullInt32]int),
		TitleType: make(map[sql.NullInt32]sql.NullString),
		StartYear: make(map[sql.NullInt32]sql.NullInt32),
		Runtime:   make(map[sql.NullInt32]int),
		AvgRating: make(map[sql.NullInt32]decimal.NullDecimal),
		GenreId:   make(map[sql.NullInt32]int),
		Genre:     make(map[sql.NullInt32]sql.NullString),
		MemberId:  make(map[sql.NullInt32]int),
		Role:      make(map[sql.NullInt32]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		// titleID
		titleID, ok := maps.TitleID[elem.BirthYear]

		if !ok {
			maps.TitleID[elem.BirthYear] = elem.TitleID
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleID != titleID {

				isValid[0] = false
			}
		}

		// type
		titleType, ok := maps.TitleType[elem.BirthYear]

		if !ok {
			maps.TitleType[elem.BirthYear] = elem.TitleType
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.TitleType.String != titleType.String {
				isValid[1] = false
			}
		}

		// startYear
		startYear, ok := maps.StartYear[elem.BirthYear]

		if !ok {
			maps.StartYear[elem.BirthYear] = elem.StartYear
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.StartYear != startYear {
				isValid[2] = false
			}
		}

		// runtime
		runtime, ok := maps.Runtime[elem.BirthYear]

		if !ok {
			maps.Runtime[elem.BirthYear] = elem.Runtime
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Runtime == runtime {
				isValid[3] = false
			}
		}

		// avgRating
		avgRating, ok := maps.AvgRating[elem.BirthYear]

		if !ok {
			maps.AvgRating[elem.BirthYear] = elem.AvgRating
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.AvgRating != avgRating {
				isValid[4] = false
			}
		}

		// genre_id
		genreID, ok := maps.GenreId[elem.BirthYear]

		if !ok {
			maps.GenreId[elem.BirthYear] = elem.GenreId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.GenreId != genreID {
				isValid[5] = false
			}
		}

		// genre
		genre, ok := maps.Genre[elem.BirthYear]

		if !ok {
			maps.Genre[elem.BirthYear] = elem.Genre
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Genre != genre {
				isValid[6] = false
			}
		}

		// member_id
		memberID, ok := maps.MemberId[elem.BirthYear]

		if !ok {
			maps.MemberId[elem.BirthYear] = elem.MemberId
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.MemberId != memberID {
				isValid[7] = false
			}
		}

		// role
		role, ok := maps.Role[elem.BirthYear]

		if !ok {
			maps.Role[elem.BirthYear] = elem.Role
		} else {
			// Since they differ, this is not a valid functional dependency
			if elem.Role != role {
				isValid[8] = false
			}
		}
	}

	header := "birthYear->"

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				println(header + "movieID")
			case 1:
				println(header + "type")
			case 2:
				println(header + "startYear")
			case 3:
				println(header + "runtimeMinutes")
			case 4:
				println(header + "avgRating")
			case 5:
				println(header + "genre_id")
			case 6:
				println(header + "genre")
			case 7:
				println(header + "member_id")
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

	wg.Add(9)

	go checkMovieID(wg, data)
	go checkType(wg, data)
	go checkStartYear(wg, data)
	go checkRuntime(wg, data)
	go checkAvgRating(wg, data)
	go checkGenreId(wg, data)
	go checkGenre(wg, data)
	go checkMemberID(wg, data)
	go checkBirthYear(wg, data)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
