package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)

type movieTitleActorNaive struct {
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

type genericMapsNaive struct {
	TitleID   map[string]int
	TitleType map[string]sql.NullString
	StartYear map[string]sql.NullInt32
	Runtime   map[string]int
	AvgRating map[string]decimal.NullDecimal
	GenreId   map[string]int
	Genre     map[string]sql.NullString
	MemberId  map[string]int
	BirthYear map[string]sql.NullInt32
	Role      map[string]string
}

func readInDataNaive() []movieTitleActorNaive {

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

	var data []movieTitleActorNaive

	defer rows.Close()

	defer rows.Close()

	for rows.Next() {
		var m = movieTitleActorNaive{}
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

func getValueByColumnNum(loc int, elem movieTitleActorNaive) string {
	switch loc {
	case 0:
		return strconv.Itoa(elem.TitleID)
	case 1:
		return elem.TitleType.String
	case 2:
		return strconv.Itoa(int(elem.StartYear.Int32))
	case 3:
		return strconv.Itoa(elem.Runtime)
	case 4:
		return elem.AvgRating.Decimal.String()
	case 5:
		return strconv.Itoa(elem.GenreId)
	case 6:
		return elem.Genre.String
	case 7:
		return strconv.Itoa(elem.MemberId)
	case 8:
		return strconv.Itoa(int(elem.BirthYear.Int32))
	case 9:
		return elem.Role
	}
	return ""
}

func checkOneOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	maps := genericMapsNaive{
		TitleID:   make(map[string]int),
		TitleType: make(map[string]sql.NullString),
		StartYear: make(map[string]sql.NullInt32),
		Runtime:   make(map[string]int),
		AvgRating: make(map[string]decimal.NullDecimal),
		GenreId:   make(map[string]int),
		Genre:     make(map[string]sql.NullString),
		MemberId:  make(map[string]int),
		BirthYear: make(map[string]sql.NullInt32),
		Role:      make(map[string]string),
	}

	// All default to being valid functional dependencies. Change to false once we discover they are not
	isValid := []bool{true, true, true, true, true, true, true, true, true, true}

	// Iterate through each column in db
	for group := 0; group < 10; group++ {

		// Iterate through each row in db
		for _, elem := range data {

			bytes := md5.Sum([]byte(getValueByColumnNum(group, elem)))
			key := string(bytes[:])

			// titleID
			titleID, ok := maps.TitleID[key]

			if !ok {
				maps.TitleID[key] = elem.TitleID
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.TitleID != titleID {

					isValid[0] = false
				}
			}

			// type
			titleType, ok := maps.TitleType[key]

			if !ok {
				maps.TitleType[key] = elem.TitleType
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.TitleType.String != titleType.String {

					isValid[0] = false
				}
			}

			// startYear
			startYear, ok := maps.StartYear[key]

			if !ok {
				maps.StartYear[key] = elem.StartYear
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.StartYear.Int32 != startYear.Int32 {
					isValid[1] = false
				}
			}

			// runtimeMinutes
			runtimeMinutes, ok := maps.Runtime[key]

			if !ok {
				maps.Runtime[key] = elem.Runtime
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Runtime != runtimeMinutes {
					isValid[2] = false
				}
			}

			// avgRating
			avgRating, ok := maps.AvgRating[key]

			if !ok {
				maps.AvgRating[key] = elem.AvgRating
			} else {
				// Since they differ, this is not a valid functional dependency
				if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
					isValid[3] = false
				}
			}

			// genre_id
			genreID, ok := maps.GenreId[key]

			if !ok {
				maps.GenreId[key] = elem.GenreId
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.GenreId != genreID {
					isValid[4] = false
				}
			}

			// genre
			genre, ok := maps.Genre[key]

			if !ok {
				maps.Genre[key] = elem.Genre
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Genre.String != genre.String {
					isValid[5] = false
				}
			}

			// member_id
			memberID, ok := maps.MemberId[key]

			if !ok {
				maps.MemberId[key] = elem.MemberId
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.MemberId != memberID {
					isValid[6] = false
				}
			}

			// birthYear
			birthYear, ok := maps.BirthYear[key]

			if !ok {
				maps.BirthYear[key] = elem.BirthYear
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.BirthYear.Int32 != birthYear.Int32 {
					isValid[7] = false
				}
			}

			// role
			role, ok := maps.Role[key]

			if !ok {
				maps.Role[key] = elem.Role
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Role != role {
					isValid[8] = false
				}
			}
		}
	}
}

func main() {
	start := time.Now()

	// Same as other one
	data := readInDataNaive()

	// I will spin up 9 go routines. 1 for each size of groups we'll be analyzing
	// Once that's done, I will print out text to tell us the dependencies
	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Will run naive version of all the below
	go checkOneOnLeft(wg, data)
	//go checkType(wg, data)
	//go checkStartYear(wg, data)
	//go checkRuntime(wg, data)
	//go checkAvgRating(wg, data)
	//go checkGenreId(wg, data)
	//go checkGenre(wg, data)
	//go checkMemberID(wg, data)
	//go checkBirthYear(wg, data)
	//go checkRole(wg, data)
	wg.Wait()
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
	_ = data
}
