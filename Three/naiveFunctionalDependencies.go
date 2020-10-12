package main

import (
	"context"
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

func getValueByColumnNumNaive(loc int, elem movieTitleActorNaive) string {
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

	// Iterate through each column in db
	for group := 0; group < 10; group++ {

		// All default to being valid functional dependencies. Change to false once we discover they are not
		// Reset after each group
		isValid := []bool{true, true, true, true, true, true, true, true, true, true}

		// Need to create new maps for each group too or else we'll get collisions
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

		// Iterate through each row in db
		for _, elem := range data {

			key := getValueByColumnNumNaive(group, elem)

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

					isValid[1] = false
				}
			}

			// startYear
			startYear, ok := maps.StartYear[key]

			if !ok {
				maps.StartYear[key] = elem.StartYear
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.StartYear.Int32 != startYear.Int32 {
					isValid[2] = false
				}
			}

			// runtimeMinutes
			runtimeMinutes, ok := maps.Runtime[key]

			if !ok {
				maps.Runtime[key] = elem.Runtime
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Runtime != runtimeMinutes {
					isValid[3] = false
				}
			}

			// avgRating
			avgRating, ok := maps.AvgRating[key]

			if !ok {
				maps.AvgRating[key] = elem.AvgRating
			} else {
				// Since they differ, this is not a valid functional dependency
				if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
					isValid[4] = false
				}
			}

			// genre_id
			genreID, ok := maps.GenreId[key]

			if !ok {
				maps.GenreId[key] = elem.GenreId
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.GenreId != genreID {
					isValid[5] = false
				}
			}

			// genre
			genre, ok := maps.Genre[key]

			if !ok {
				maps.Genre[key] = elem.Genre
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Genre.String != genre.String {
					isValid[6] = false
				}
			}

			// member_id
			memberID, ok := maps.MemberId[key]

			if !ok {
				maps.MemberId[key] = elem.MemberId
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.MemberId != memberID {
					isValid[7] = false
				}
			}

			// birthYear
			birthYear, ok := maps.BirthYear[key]

			if !ok {
				maps.BirthYear[key] = elem.BirthYear
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.BirthYear.Int32 != birthYear.Int32 {
					isValid[8] = false
				}
			}

			// role
			role, ok := maps.Role[key]

			if !ok {
				maps.Role[key] = elem.Role
			} else {
				// Since they differ, this is not a valid functional dependency
				if elem.Role != role {
					isValid[9] = false
				}
			}
		}

		header := ""

		switch group {
		case 0:
			header += "movieID->"
		case 1:
			header += "type->"
		case 2:
			header += "startYear->"
		case 3:
			header += "runtimeMinutes->"
		case 4:
			header += "avgRating->"
		case 5:
			header += "genre_id->"
		case 6:
			header += "genre->"
		case 7:
			header += "member_id->"
		case 8:
			header += "birthYear->"
		case 9:
			header += "role->"
		}

		for idx, valid := range isValid {
			if valid && idx != group {
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
					println(header + "birthYear")
				case 9:
					println(header + "role")
				}
			}
		}
	}

	fmt.Println("Check One Done")
}

func checkTwoOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			// All default to being valid functional dependencies. Change to false once we discover they are not
			// Reset after each group
			isValid := []bool{true, true, true, true, true, true, true, true, true, true}

			// Need to create new maps for each group too or else we'll get collisions
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

			// Iterate through each row in db
			for _, elem := range data {

				key := getValueByColumnNumNaive(groupOne, elem)
				key += getValueByColumnNumNaive(groupTwo, elem)

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

						isValid[1] = false
					}
				}

				// startYear
				startYear, ok := maps.StartYear[key]

				if !ok {
					maps.StartYear[key] = elem.StartYear
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.StartYear.Int32 != startYear.Int32 {
						isValid[2] = false
					}
				}

				// runtimeMinutes
				runtimeMinutes, ok := maps.Runtime[key]

				if !ok {
					maps.Runtime[key] = elem.Runtime
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.Runtime != runtimeMinutes {
						isValid[3] = false
					}
				}

				// avgRating
				avgRating, ok := maps.AvgRating[key]

				if !ok {
					maps.AvgRating[key] = elem.AvgRating
				} else {
					// Since they differ, this is not a valid functional dependency
					if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
						isValid[4] = false
					}
				}

				// genre_id
				genreID, ok := maps.GenreId[key]

				if !ok {
					maps.GenreId[key] = elem.GenreId
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.GenreId != genreID {
						isValid[5] = false
					}
				}

				// genre
				genre, ok := maps.Genre[key]

				if !ok {
					maps.Genre[key] = elem.Genre
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.Genre.String != genre.String {
						isValid[6] = false
					}
				}

				// member_id
				memberID, ok := maps.MemberId[key]

				if !ok {
					maps.MemberId[key] = elem.MemberId
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.MemberId != memberID {
						isValid[7] = false
					}
				}

				// birthYear
				birthYear, ok := maps.BirthYear[key]

				if !ok {
					maps.BirthYear[key] = elem.BirthYear
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.BirthYear.Int32 != birthYear.Int32 {
						isValid[8] = false
					}
				}

				// role
				role, ok := maps.Role[key]

				if !ok {
					maps.Role[key] = elem.Role
				} else {
					// Since they differ, this is not a valid functional dependency
					if elem.Role != role {
						isValid[9] = false
					}
				}
			}

			header := ""

			switch groupOne {
			case 0:
				header += "movieID,"
			case 1:
				header += "type,"
			case 2:
				header += "startYear,"
			case 3:
				header += "runtimeMinutes,"
			case 4:
				header += "avgRating,"
			case 5:
				header += "genre_id,"
			case 6:
				header += "genre,"
			case 7:
				header += "member_id,"
			case 8:
				header += "birthYear,"
			case 9:
				header += "role,"
			}

			switch groupTwo {
			case 0:
				header += "movieID->"
			case 1:
				header += "type->"
			case 2:
				header += "startYear->"
			case 3:
				header += "runtimeMinutes->"
			case 4:
				header += "avgRating->"
			case 5:
				header += "genre_id->"
			case 6:
				header += "genre->"
			case 7:
				header += "member_id->"
			case 8:
				header += "birthYear->"
			case 9:
				header += "role->"
			}

			for idx, valid := range isValid {
				if valid && idx != groupOne && idx != groupTwo {
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
						println(header + "birthYear")
					case 9:
						println(header + "role")
					}
				}
			}
		}
	}

	fmt.Println("Check Two Done")
}

func checkThreeOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				// All default to being valid functional dependencies. Change to false once we discover they are not
				// Reset after each group
				isValid := []bool{true, true, true, true, true, true, true, true, true, true}

				// Need to create new maps for each group too or else we'll get collisions
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

				// Iterate through each row in db
				for _, elem := range data {

					key := getValueByColumnNumNaive(groupOne, elem)
					key += getValueByColumnNumNaive(groupTwo, elem)
					key += getValueByColumnNumNaive(groupThree, elem)

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

							isValid[1] = false
						}
					}

					// startYear
					startYear, ok := maps.StartYear[key]

					if !ok {
						maps.StartYear[key] = elem.StartYear
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.StartYear.Int32 != startYear.Int32 {
							isValid[2] = false
						}
					}

					// runtimeMinutes
					runtimeMinutes, ok := maps.Runtime[key]

					if !ok {
						maps.Runtime[key] = elem.Runtime
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Runtime != runtimeMinutes {
							isValid[3] = false
						}
					}

					// avgRating
					avgRating, ok := maps.AvgRating[key]

					if !ok {
						maps.AvgRating[key] = elem.AvgRating
					} else {
						// Since they differ, this is not a valid functional dependency
						if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
							isValid[4] = false
						}
					}

					// genre_id
					genreID, ok := maps.GenreId[key]

					if !ok {
						maps.GenreId[key] = elem.GenreId
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.GenreId != genreID {
							isValid[5] = false
						}
					}

					// genre
					genre, ok := maps.Genre[key]

					if !ok {
						maps.Genre[key] = elem.Genre
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Genre.String != genre.String {
							isValid[6] = false
						}
					}

					// member_id
					memberID, ok := maps.MemberId[key]

					if !ok {
						maps.MemberId[key] = elem.MemberId
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.MemberId != memberID {
							isValid[7] = false
						}
					}

					// birthYear
					birthYear, ok := maps.BirthYear[key]

					if !ok {
						maps.BirthYear[key] = elem.BirthYear
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.BirthYear.Int32 != birthYear.Int32 {
							isValid[8] = false
						}
					}

					// role
					role, ok := maps.Role[key]

					if !ok {
						maps.Role[key] = elem.Role
					} else {
						// Since they differ, this is not a valid functional dependency
						if elem.Role != role {
							isValid[9] = false
						}
					}
				}

				header := ""

				switch groupOne {
				case 0:
					header += "movieID,"
				case 1:
					header += "type,"
				case 2:
					header += "startYear,"
				case 3:
					header += "runtimeMinutes,"
				case 4:
					header += "avgRating,"
				case 5:
					header += "genre_id,"
				case 6:
					header += "genre,"
				case 7:
					header += "member_id,"
				case 8:
					header += "birthYear,"
				case 9:
					header += "role,"
				}

				switch groupTwo {
				case 0:
					header += "movieID,"
				case 1:
					header += "type,"
				case 2:
					header += "startYear,"
				case 3:
					header += "runtimeMinutes,"
				case 4:
					header += "avgRating,"
				case 5:
					header += "genre_id,"
				case 6:
					header += "genre,"
				case 7:
					header += "member_id,"
				case 8:
					header += "birthYear,"
				case 9:
					header += "role,"
				}

				switch groupThree {
				case 0:
					header += "movieID->"
				case 1:
					header += "type->"
				case 2:
					header += "startYear->"
				case 3:
					header += "runtimeMinutes->"
				case 4:
					header += "avgRating->"
				case 5:
					header += "genre_id->"
				case 6:
					header += "genre->"
				case 7:
					header += "member_id->"
				case 8:
					header += "birthYear->"
				case 9:
					header += "role->"
				}

				for idx, valid := range isValid {
					if valid && idx != groupOne && idx != groupTwo && idx != groupThree {
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
							println(header + "birthYear")
						case 9:
							println(header + "role")
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Three Done")
}

func checkFourOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				for groupFour := 0; groupFour < 10; groupFour++ {

					// All default to being valid functional dependencies. Change to false once we discover they are not
					// Reset after each group
					isValid := []bool{true, true, true, true, true, true, true, true, true, true}

					// Need to create new maps for each group too or else we'll get collisions
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

					// Iterate through each row in db
					for _, elem := range data {

						key := getValueByColumnNumNaive(groupOne, elem)
						key += getValueByColumnNumNaive(groupTwo, elem)
						key += getValueByColumnNumNaive(groupThree, elem)
						key += getValueByColumnNumNaive(groupFour, elem)

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

								isValid[1] = false
							}
						}

						// startYear
						startYear, ok := maps.StartYear[key]

						if !ok {
							maps.StartYear[key] = elem.StartYear
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.StartYear.Int32 != startYear.Int32 {
								isValid[2] = false
							}
						}

						// runtimeMinutes
						runtimeMinutes, ok := maps.Runtime[key]

						if !ok {
							maps.Runtime[key] = elem.Runtime
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.Runtime != runtimeMinutes {
								isValid[3] = false
							}
						}

						// avgRating
						avgRating, ok := maps.AvgRating[key]

						if !ok {
							maps.AvgRating[key] = elem.AvgRating
						} else {
							// Since they differ, this is not a valid functional dependency
							if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
								isValid[4] = false
							}
						}

						// genre_id
						genreID, ok := maps.GenreId[key]

						if !ok {
							maps.GenreId[key] = elem.GenreId
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.GenreId != genreID {
								isValid[5] = false
							}
						}

						// genre
						genre, ok := maps.Genre[key]

						if !ok {
							maps.Genre[key] = elem.Genre
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.Genre.String != genre.String {
								isValid[6] = false
							}
						}

						// member_id
						memberID, ok := maps.MemberId[key]

						if !ok {
							maps.MemberId[key] = elem.MemberId
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.MemberId != memberID {
								isValid[7] = false
							}
						}

						// birthYear
						birthYear, ok := maps.BirthYear[key]

						if !ok {
							maps.BirthYear[key] = elem.BirthYear
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.BirthYear.Int32 != birthYear.Int32 {
								isValid[8] = false
							}
						}

						// role
						role, ok := maps.Role[key]

						if !ok {
							maps.Role[key] = elem.Role
						} else {
							// Since they differ, this is not a valid functional dependency
							if elem.Role != role {
								isValid[9] = false
							}
						}
					}

					header := ""

					switch groupOne {
					case 0:
						header += "movieID,"
					case 1:
						header += "type,"
					case 2:
						header += "startYear,"
					case 3:
						header += "runtimeMinutes,"
					case 4:
						header += "avgRating,"
					case 5:
						header += "genre_id,"
					case 6:
						header += "genre,"
					case 7:
						header += "member_id,"
					case 8:
						header += "birthYear,"
					case 9:
						header += "role,"
					}

					switch groupTwo {
					case 0:
						header += "movieID,"
					case 1:
						header += "type,"
					case 2:
						header += "startYear,"
					case 3:
						header += "runtimeMinutes,"
					case 4:
						header += "avgRating,"
					case 5:
						header += "genre_id,"
					case 6:
						header += "genre,"
					case 7:
						header += "member_id,"
					case 8:
						header += "birthYear,"
					case 9:
						header += "role,"
					}

					switch groupThree {
					case 0:
						header += "movieID,"
					case 1:
						header += "type,"
					case 2:
						header += "startYear,"
					case 3:
						header += "runtimeMinutes,"
					case 4:
						header += "avgRating,"
					case 5:
						header += "genre_id,"
					case 6:
						header += "genre,"
					case 7:
						header += "member_id,"
					case 8:
						header += "birthYear,"
					case 9:
						header += "role,"
					}

					switch groupFour {
					case 0:
						header += "movieID->"
					case 1:
						header += "type->"
					case 2:
						header += "startYear->"
					case 3:
						header += "runtimeMinutes->"
					case 4:
						header += "avgRating->"
					case 5:
						header += "genre_id->"
					case 6:
						header += "genre->"
					case 7:
						header += "member_id->"
					case 8:
						header += "birthYear->"
					case 9:
						header += "role->"

					}

					for idx, valid := range isValid {
						if valid && idx != groupOne && idx != groupTwo && idx != groupThree && idx != groupFour {
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
								println(header + "birthYear")
							case 9:
								println(header + "role")
							}
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Four Done")
}

func checkFiveOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				for groupFour := 0; groupFour < 10; groupFour++ {

					for groupFive := 0; groupFive < 10; groupFive++ {

						// All default to being valid functional dependencies. Change to false once we discover they are not
						// Reset after each group
						isValid := []bool{true, true, true, true, true, true, true, true, true, true}

						// Need to create new maps for each group too or else we'll get collisions
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

						// Iterate through each row in db
						for _, elem := range data {

							key := getValueByColumnNumNaive(groupOne, elem)
							key += getValueByColumnNumNaive(groupTwo, elem)
							key += getValueByColumnNumNaive(groupThree, elem)
							key += getValueByColumnNumNaive(groupFour, elem)
							key += getValueByColumnNumNaive(groupFive, elem)

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

									isValid[1] = false
								}
							}

							// startYear
							startYear, ok := maps.StartYear[key]

							if !ok {
								maps.StartYear[key] = elem.StartYear
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.StartYear.Int32 != startYear.Int32 {
									isValid[2] = false
								}
							}

							// runtimeMinutes
							runtimeMinutes, ok := maps.Runtime[key]

							if !ok {
								maps.Runtime[key] = elem.Runtime
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.Runtime != runtimeMinutes {
									isValid[3] = false
								}
							}

							// avgRating
							avgRating, ok := maps.AvgRating[key]

							if !ok {
								maps.AvgRating[key] = elem.AvgRating
							} else {
								// Since they differ, this is not a valid functional dependency
								if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
									isValid[4] = false
								}
							}

							// genre_id
							genreID, ok := maps.GenreId[key]

							if !ok {
								maps.GenreId[key] = elem.GenreId
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.GenreId != genreID {
									isValid[5] = false
								}
							}

							// genre
							genre, ok := maps.Genre[key]

							if !ok {
								maps.Genre[key] = elem.Genre
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.Genre.String != genre.String {
									isValid[6] = false
								}
							}

							// member_id
							memberID, ok := maps.MemberId[key]

							if !ok {
								maps.MemberId[key] = elem.MemberId
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.MemberId != memberID {
									isValid[7] = false
								}
							}

							// birthYear
							birthYear, ok := maps.BirthYear[key]

							if !ok {
								maps.BirthYear[key] = elem.BirthYear
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.BirthYear.Int32 != birthYear.Int32 {
									isValid[8] = false
								}
							}

							// role
							role, ok := maps.Role[key]

							if !ok {
								maps.Role[key] = elem.Role
							} else {
								// Since they differ, this is not a valid functional dependency
								if elem.Role != role {
									isValid[9] = false
								}
							}
						}

						header := ""

						switch groupOne {
						case 0:
							header += "movieID,"
						case 1:
							header += "type,"
						case 2:
							header += "startYear,"
						case 3:
							header += "runtimeMinutes,"
						case 4:
							header += "avgRating,"
						case 5:
							header += "genre_id,"
						case 6:
							header += "genre,"
						case 7:
							header += "member_id,"
						case 8:
							header += "birthYear,"
						case 9:
							header += "role,"
						}

						switch groupTwo {
						case 0:
							header += "movieID,"
						case 1:
							header += "type,"
						case 2:
							header += "startYear,"
						case 3:
							header += "runtimeMinutes,"
						case 4:
							header += "avgRating,"
						case 5:
							header += "genre_id,"
						case 6:
							header += "genre,"
						case 7:
							header += "member_id,"
						case 8:
							header += "birthYear,"
						case 9:
							header += "role,"
						}

						switch groupThree {
						case 0:
							header += "movieID,"
						case 1:
							header += "type,"
						case 2:
							header += "startYear,"
						case 3:
							header += "runtimeMinutes,"
						case 4:
							header += "avgRating,"
						case 5:
							header += "genre_id,"
						case 6:
							header += "genre,"
						case 7:
							header += "member_id,"
						case 8:
							header += "birthYear,"
						case 9:
							header += "role,"
						}

						switch groupFour {
						case 0:
							header += "movieID,"
						case 1:
							header += "type,"
						case 2:
							header += "startYear,"
						case 3:
							header += "runtimeMinutes,"
						case 4:
							header += "avgRating,"
						case 5:
							header += "genre_id,"
						case 6:
							header += "genre,"
						case 7:
							header += "member_id,"
						case 8:
							header += "birthYear,"
						case 9:
							header += "role,"
						}

						switch groupFive {
						case 0:
							header += "movieID->"
						case 1:
							header += "type->"
						case 2:
							header += "startYear->"
						case 3:
							header += "runtimeMinutes->"
						case 4:
							header += "avgRating->"
						case 5:
							header += "genre_id->"
						case 6:
							header += "genre->"
						case 7:
							header += "member_id->"
						case 8:
							header += "birthYear->"
						case 9:
							header += "role->"

						}

						for idx, valid := range isValid {
							if valid && idx != groupOne && idx != groupTwo && idx != groupThree && idx != groupFour && idx != groupFive {
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
									println(header + "birthYear")
								case 9:
									println(header + "role")
								}
							}
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Five Done")
}

func checkSixOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				for groupFour := 0; groupFour < 10; groupFour++ {

					for groupFive := 0; groupFive < 10; groupFive++ {

						for groupSix := 0; groupSix < 10; groupSix++ {

							// All default to being valid functional dependencies. Change to false once we discover they are not
							// Reset after each group
							isValid := []bool{true, true, true, true, true, true, true, true, true, true}

							// Need to create new maps for each group too or else we'll get collisions
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

							// Iterate through each row in db
							for _, elem := range data {

								key := getValueByColumnNumNaive(groupOne, elem)
								key += getValueByColumnNumNaive(groupTwo, elem)
								key += getValueByColumnNumNaive(groupThree, elem)
								key += getValueByColumnNumNaive(groupFour, elem)
								key += getValueByColumnNumNaive(groupFive, elem)
								key += getValueByColumnNumNaive(groupSix, elem)

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

										isValid[1] = false
									}
								}

								// startYear
								startYear, ok := maps.StartYear[key]

								if !ok {
									maps.StartYear[key] = elem.StartYear
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.StartYear.Int32 != startYear.Int32 {
										isValid[2] = false
									}
								}

								// runtimeMinutes
								runtimeMinutes, ok := maps.Runtime[key]

								if !ok {
									maps.Runtime[key] = elem.Runtime
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.Runtime != runtimeMinutes {
										isValid[3] = false
									}
								}

								// avgRating
								avgRating, ok := maps.AvgRating[key]

								if !ok {
									maps.AvgRating[key] = elem.AvgRating
								} else {
									// Since they differ, this is not a valid functional dependency
									if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
										isValid[4] = false
									}
								}

								// genre_id
								genreID, ok := maps.GenreId[key]

								if !ok {
									maps.GenreId[key] = elem.GenreId
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.GenreId != genreID {
										isValid[5] = false
									}
								}

								// genre
								genre, ok := maps.Genre[key]

								if !ok {
									maps.Genre[key] = elem.Genre
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.Genre.String != genre.String {
										isValid[6] = false
									}
								}

								// member_id
								memberID, ok := maps.MemberId[key]

								if !ok {
									maps.MemberId[key] = elem.MemberId
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.MemberId != memberID {
										isValid[7] = false
									}
								}

								// birthYear
								birthYear, ok := maps.BirthYear[key]

								if !ok {
									maps.BirthYear[key] = elem.BirthYear
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.BirthYear.Int32 != birthYear.Int32 {
										isValid[8] = false
									}
								}

								// role
								role, ok := maps.Role[key]

								if !ok {
									maps.Role[key] = elem.Role
								} else {
									// Since they differ, this is not a valid functional dependency
									if elem.Role != role {
										isValid[9] = false
									}
								}
							}

							header := ""

							switch groupOne {
							case 0:
								header += "movieID,"
							case 1:
								header += "type,"
							case 2:
								header += "startYear,"
							case 3:
								header += "runtimeMinutes,"
							case 4:
								header += "avgRating,"
							case 5:
								header += "genre_id,"
							case 6:
								header += "genre,"
							case 7:
								header += "member_id,"
							case 8:
								header += "birthYear,"
							case 9:
								header += "role,"
							}

							switch groupTwo {
							case 0:
								header += "movieID,"
							case 1:
								header += "type,"
							case 2:
								header += "startYear,"
							case 3:
								header += "runtimeMinutes,"
							case 4:
								header += "avgRating,"
							case 5:
								header += "genre_id,"
							case 6:
								header += "genre,"
							case 7:
								header += "member_id,"
							case 8:
								header += "birthYear,"
							case 9:
								header += "role,"
							}

							switch groupThree {
							case 0:
								header += "movieID,"
							case 1:
								header += "type,"
							case 2:
								header += "startYear,"
							case 3:
								header += "runtimeMinutes,"
							case 4:
								header += "avgRating,"
							case 5:
								header += "genre_id,"
							case 6:
								header += "genre,"
							case 7:
								header += "member_id,"
							case 8:
								header += "birthYear,"
							case 9:
								header += "role,"
							}

							switch groupFour {
							case 0:
								header += "movieID,"
							case 1:
								header += "type,"
							case 2:
								header += "startYear,"
							case 3:
								header += "runtimeMinutes,"
							case 4:
								header += "avgRating,"
							case 5:
								header += "genre_id,"
							case 6:
								header += "genre,"
							case 7:
								header += "member_id,"
							case 8:
								header += "birthYear,"
							case 9:
								header += "role,"
							}

							switch groupFive {
							case 0:
								header += "movieID,"
							case 1:
								header += "type,"
							case 2:
								header += "startYear,"
							case 3:
								header += "runtimeMinutes->"
							case 4:
								header += "avgRating,"
							case 5:
								header += "genre_id,"
							case 6:
								header += "genre,"
							case 7:
								header += "member_id,"
							case 8:
								header += "birthYear,"
							case 9:
								header += "role,"
							}

							switch groupSix {
							case 0:
								header += "movieID->"
							case 1:
								header += "type->"
							case 2:
								header += "startYear->"
							case 3:
								header += "runtimeMinutes->"
							case 4:
								header += "avgRating->"
							case 5:
								header += "genre_id->"
							case 6:
								header += "genre->"
							case 7:
								header += "member_id->"
							case 8:
								header += "birthYear->"
							case 9:
								header += "role->"

							}

							for idx, valid := range isValid {
								if valid && idx != groupOne && idx != groupTwo && idx != groupThree &&
									idx != groupFour && idx != groupFive && idx != groupSix {
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
										println(header + "birthYear")
									case 9:
										println(header + "role")
									}
								}
							}
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Six Done")
}

func checkSevenOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				for groupFour := 0; groupFour < 10; groupFour++ {

					for groupFive := 0; groupFive < 10; groupFive++ {

						for groupSix := 0; groupSix < 10; groupSix++ {

							for groupSeven := 0; groupSeven < 10; groupSeven++ {

								// All default to being valid functional dependencies. Change to false once we discover they are not
								// Reset after each group
								isValid := []bool{true, true, true, true, true, true, true, true, true, true}

								// Need to create new maps for each group too or else we'll get collisions
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

								// Iterate through each row in db
								for _, elem := range data {

									key := getValueByColumnNumNaive(groupOne, elem)
									key += getValueByColumnNumNaive(groupTwo, elem)
									key += getValueByColumnNumNaive(groupThree, elem)
									key += getValueByColumnNumNaive(groupFour, elem)
									key += getValueByColumnNumNaive(groupFive, elem)
									key += getValueByColumnNumNaive(groupSix, elem)
									key += getValueByColumnNumNaive(groupSeven, elem)

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

											isValid[1] = false
										}
									}

									// startYear
									startYear, ok := maps.StartYear[key]

									if !ok {
										maps.StartYear[key] = elem.StartYear
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.StartYear.Int32 != startYear.Int32 {
											isValid[2] = false
										}
									}

									// runtimeMinutes
									runtimeMinutes, ok := maps.Runtime[key]

									if !ok {
										maps.Runtime[key] = elem.Runtime
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.Runtime != runtimeMinutes {
											isValid[3] = false
										}
									}

									// avgRating
									avgRating, ok := maps.AvgRating[key]

									if !ok {
										maps.AvgRating[key] = elem.AvgRating
									} else {
										// Since they differ, this is not a valid functional dependency
										if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
											isValid[4] = false
										}
									}

									// genre_id
									genreID, ok := maps.GenreId[key]

									if !ok {
										maps.GenreId[key] = elem.GenreId
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.GenreId != genreID {
											isValid[5] = false
										}
									}

									// genre
									genre, ok := maps.Genre[key]

									if !ok {
										maps.Genre[key] = elem.Genre
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.Genre.String != genre.String {
											isValid[6] = false
										}
									}

									// member_id
									memberID, ok := maps.MemberId[key]

									if !ok {
										maps.MemberId[key] = elem.MemberId
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.MemberId != memberID {
											isValid[7] = false
										}
									}

									// birthYear
									birthYear, ok := maps.BirthYear[key]

									if !ok {
										maps.BirthYear[key] = elem.BirthYear
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.BirthYear.Int32 != birthYear.Int32 {
											isValid[8] = false
										}
									}

									// role
									role, ok := maps.Role[key]

									if !ok {
										maps.Role[key] = elem.Role
									} else {
										// Since they differ, this is not a valid functional dependency
										if elem.Role != role {
											isValid[9] = false
										}
									}
								}

								header := ""

								switch groupOne {
								case 0:
									header += "movieID,"
								case 1:
									header += "type,"
								case 2:
									header += "startYear,"
								case 3:
									header += "runtimeMinutes,"
								case 4:
									header += "avgRating,"
								case 5:
									header += "genre_id,"
								case 6:
									header += "genre,"
								case 7:
									header += "member_id,"
								case 8:
									header += "birthYear,"
								case 9:
									header += "role,"
								}

								switch groupTwo {
								case 0:
									header += "movieID,"
								case 1:
									header += "type,"
								case 2:
									header += "startYear,"
								case 3:
									header += "runtimeMinutes,"
								case 4:
									header += "avgRating,"
								case 5:
									header += "genre_id,"
								case 6:
									header += "genre,"
								case 7:
									header += "member_id,"
								case 8:
									header += "birthYear,"
								case 9:
									header += "role,"
								}

								switch groupThree {
								case 0:
									header += "movieID,"
								case 1:
									header += "type,"
								case 2:
									header += "startYear,"
								case 3:
									header += "runtimeMinutes,"
								case 4:
									header += "avgRating,"
								case 5:
									header += "genre_id,"
								case 6:
									header += "genre,"
								case 7:
									header += "member_id,"
								case 8:
									header += "birthYear,"
								case 9:
									header += "role,"
								}

								switch groupFour {
								case 0:
									header += "movieID,"
								case 1:
									header += "type,"
								case 2:
									header += "startYear,"
								case 3:
									header += "runtimeMinutes,"
								case 4:
									header += "avgRating,"
								case 5:
									header += "genre_id,"
								case 6:
									header += "genre,"
								case 7:
									header += "member_id,"
								case 8:
									header += "birthYear,"
								case 9:
									header += "role,"
								}

								switch groupFive {
								case 0:
									header += "movieID,"
								case 1:
									header += "type,"
								case 2:
									header += "startYear,"
								case 3:
									header += "runtimeMinutes->"
								case 4:
									header += "avgRating,"
								case 5:
									header += "genre_id,"
								case 6:
									header += "genre,"
								case 7:
									header += "member_id,"
								case 8:
									header += "birthYear,"
								case 9:
									header += "role,"
								}

								switch groupSix {
								case 0:
									header += "movieID,"
								case 1:
									header += "type,"
								case 2:
									header += "startYear,"
								case 3:
									header += "runtimeMinutes,"
								case 4:
									header += "avgRating,"
								case 5:
									header += "genre_id,"
								case 6:
									header += "genre,"
								case 7:
									header += "member_id,"
								case 8:
									header += "birthYear,"
								case 9:
									header += "role,"
								}

								switch groupSeven {
								case 0:
									header += "movieID->"
								case 1:
									header += "type->"
								case 2:
									header += "startYear->"
								case 3:
									header += "runtimeMinutes->"
								case 4:
									header += "avgRating->"
								case 5:
									header += "genre_id->"
								case 6:
									header += "genre->"
								case 7:
									header += "member_id->"
								case 8:
									header += "birthYear->"
								case 9:
									header += "role->"
								}

								for idx, valid := range isValid {
									if valid && idx != groupOne && idx != groupTwo && idx != groupThree &&
										idx != groupFour && idx != groupFive && idx != groupSix &&
										idx != groupSeven {
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
											println(header + "birthYear")
										case 9:
											println(header + "role")
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Seven Done")
}

func checkEightOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				for groupFour := 0; groupFour < 10; groupFour++ {

					for groupFive := 0; groupFive < 10; groupFive++ {

						for groupSix := 0; groupSix < 10; groupSix++ {

							for groupSeven := 0; groupSeven < 10; groupSeven++ {

								for groupEight := 0; groupEight < 10; groupEight++ {

									// All default to being valid functional dependencies. Change to false once we discover they are not
									// Reset after each group
									isValid := []bool{true, true, true, true, true, true, true, true, true, true}

									// Need to create new maps for each group too or else we'll get collisions
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

									// Iterate through each row in db
									for _, elem := range data {

										key := getValueByColumnNumNaive(groupOne, elem)
										key += getValueByColumnNumNaive(groupTwo, elem)
										key += getValueByColumnNumNaive(groupThree, elem)
										key += getValueByColumnNumNaive(groupFour, elem)
										key += getValueByColumnNumNaive(groupFive, elem)
										key += getValueByColumnNumNaive(groupSix, elem)
										key += getValueByColumnNumNaive(groupSeven, elem)
										key += getValueByColumnNumNaive(groupEight, elem)

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

												isValid[1] = false
											}
										}

										// startYear
										startYear, ok := maps.StartYear[key]

										if !ok {
											maps.StartYear[key] = elem.StartYear
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.StartYear.Int32 != startYear.Int32 {
												isValid[2] = false
											}
										}

										// runtimeMinutes
										runtimeMinutes, ok := maps.Runtime[key]

										if !ok {
											maps.Runtime[key] = elem.Runtime
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.Runtime != runtimeMinutes {
												isValid[3] = false
											}
										}

										// avgRating
										avgRating, ok := maps.AvgRating[key]

										if !ok {
											maps.AvgRating[key] = elem.AvgRating
										} else {
											// Since they differ, this is not a valid functional dependency
											if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
												isValid[4] = false
											}
										}

										// genre_id
										genreID, ok := maps.GenreId[key]

										if !ok {
											maps.GenreId[key] = elem.GenreId
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.GenreId != genreID {
												isValid[5] = false
											}
										}

										// genre
										genre, ok := maps.Genre[key]

										if !ok {
											maps.Genre[key] = elem.Genre
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.Genre.String != genre.String {
												isValid[6] = false
											}
										}

										// member_id
										memberID, ok := maps.MemberId[key]

										if !ok {
											maps.MemberId[key] = elem.MemberId
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.MemberId != memberID {
												isValid[7] = false
											}
										}

										// birthYear
										birthYear, ok := maps.BirthYear[key]

										if !ok {
											maps.BirthYear[key] = elem.BirthYear
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.BirthYear.Int32 != birthYear.Int32 {
												isValid[8] = false
											}
										}

										// role
										role, ok := maps.Role[key]

										if !ok {
											maps.Role[key] = elem.Role
										} else {
											// Since they differ, this is not a valid functional dependency
											if elem.Role != role {
												isValid[9] = false
											}
										}
									}

									header := ""

									switch groupOne {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes,"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupTwo {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes,"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupThree {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes,"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupFour {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes,"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupFive {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes->"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupSix {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes,"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupSeven {
									case 0:
										header += "movieID,"
									case 1:
										header += "type,"
									case 2:
										header += "startYear,"
									case 3:
										header += "runtimeMinutes,"
									case 4:
										header += "avgRating,"
									case 5:
										header += "genre_id,"
									case 6:
										header += "genre,"
									case 7:
										header += "member_id,"
									case 8:
										header += "birthYear,"
									case 9:
										header += "role,"
									}

									switch groupEight {
									case 0:
										header += "movieID->"
									case 1:
										header += "type->"
									case 2:
										header += "startYear->"
									case 3:
										header += "runtimeMinutes->"
									case 4:
										header += "avgRating->"
									case 5:
										header += "genre_id->"
									case 6:
										header += "genre->"
									case 7:
										header += "member_id->"
									case 8:
										header += "birthYear->"
									case 9:
										header += "role->"
									}

									for idx, valid := range isValid {
										if valid && idx != groupOne && idx != groupTwo && idx != groupThree &&
											idx != groupFour && idx != groupFive && idx != groupSix &&
											idx != groupSeven && idx != groupEight {
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
												println(header + "birthYear")
											case 9:
												println(header + "role")
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Eight Done")
}

func checkNineOnLeft(wg *sync.WaitGroup, data []movieTitleActorNaive) {

	defer wg.Done()

	// Iterate through each column in db
	for groupOne := 0; groupOne < 10; groupOne++ {

		for groupTwo := 0; groupTwo < 10; groupTwo++ {

			for groupThree := 0; groupThree < 10; groupThree++ {

				for groupFour := 0; groupFour < 10; groupFour++ {

					for groupFive := 0; groupFive < 10; groupFive++ {

						for groupSix := 0; groupSix < 10; groupSix++ {

							for groupSeven := 0; groupSeven < 10; groupSeven++ {

								for groupEight := 0; groupEight < 10; groupEight++ {

									for groupNine := 0; groupNine < 10; groupNine++ {

										// All default to being valid functional dependencies. Change to false once we discover they are not
										// Reset after each group
										isValid := []bool{true, true, true, true, true, true, true, true, true, true}

										// Need to create new maps for each group too or else we'll get collisions
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

										// Iterate through each row in db
										for _, elem := range data {

											key := getValueByColumnNumNaive(groupOne, elem)
											key += getValueByColumnNumNaive(groupTwo, elem)
											key += getValueByColumnNumNaive(groupThree, elem)
											key += getValueByColumnNumNaive(groupFour, elem)
											key += getValueByColumnNumNaive(groupFive, elem)
											key += getValueByColumnNumNaive(groupSix, elem)
											key += getValueByColumnNumNaive(groupSeven, elem)
											key += getValueByColumnNumNaive(groupEight, elem)
											key += getValueByColumnNumNaive(groupNine, elem)

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

													isValid[1] = false
												}
											}

											// startYear
											startYear, ok := maps.StartYear[key]

											if !ok {
												maps.StartYear[key] = elem.StartYear
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.StartYear.Int32 != startYear.Int32 {
													isValid[2] = false
												}
											}

											// runtimeMinutes
											runtimeMinutes, ok := maps.Runtime[key]

											if !ok {
												maps.Runtime[key] = elem.Runtime
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.Runtime != runtimeMinutes {
													isValid[3] = false
												}
											}

											// avgRating
											avgRating, ok := maps.AvgRating[key]

											if !ok {
												maps.AvgRating[key] = elem.AvgRating
											} else {
												// Since they differ, this is not a valid functional dependency
												if !elem.AvgRating.Decimal.Equal(avgRating.Decimal) {
													isValid[4] = false
												}
											}

											// genre_id
											genreID, ok := maps.GenreId[key]

											if !ok {
												maps.GenreId[key] = elem.GenreId
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.GenreId != genreID {
													isValid[5] = false
												}
											}

											// genre
											genre, ok := maps.Genre[key]

											if !ok {
												maps.Genre[key] = elem.Genre
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.Genre.String != genre.String {
													isValid[6] = false
												}
											}

											// member_id
											memberID, ok := maps.MemberId[key]

											if !ok {
												maps.MemberId[key] = elem.MemberId
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.MemberId != memberID {
													isValid[7] = false
												}
											}

											// birthYear
											birthYear, ok := maps.BirthYear[key]

											if !ok {
												maps.BirthYear[key] = elem.BirthYear
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.BirthYear.Int32 != birthYear.Int32 {
													isValid[8] = false
												}
											}

											// role
											role, ok := maps.Role[key]

											if !ok {
												maps.Role[key] = elem.Role
											} else {
												// Since they differ, this is not a valid functional dependency
												if elem.Role != role {
													isValid[9] = false
												}
											}
										}

										header := ""

										switch groupOne {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupTwo {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupThree {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupFour {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupFive {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes->"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupSix {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupSeven {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupEight {
										case 0:
											header += "movieID,"
										case 1:
											header += "type,"
										case 2:
											header += "startYear,"
										case 3:
											header += "runtimeMinutes,"
										case 4:
											header += "avgRating,"
										case 5:
											header += "genre_id,"
										case 6:
											header += "genre,"
										case 7:
											header += "member_id,"
										case 8:
											header += "birthYear,"
										case 9:
											header += "role,"
										}

										switch groupNine {
										case 0:
											header += "movieID->"
										case 1:
											header += "type->"
										case 2:
											header += "startYear->"
										case 3:
											header += "runtimeMinutes->"
										case 4:
											header += "avgRating->"
										case 5:
											header += "genre_id->"
										case 6:
											header += "genre->"
										case 7:
											header += "member_id->"
										case 8:
											header += "birthYear->"
										case 9:
											header += "role->"
										}

										for idx, valid := range isValid {
											if valid && idx != groupOne && idx != groupTwo && idx != groupThree &&
												idx != groupFour && idx != groupFive && idx != groupSix &&
												idx != groupSeven && idx != groupEight && idx != groupNine {
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
													println(header + "birthYear")
												case 9:
													println(header + "role")
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	fmt.Println("Check Nine Done")
}

func main() {
	// This program works by brute force checking all the functional dependencies. While it is parallelized, the
	// algorithm itself isn't optimized at all in the spirit of the assignment. We check groups of 1-9 on the left
	// always one on the right. I use a key of the string representation of all the fields appended together. This is
	// fast, easy and guaranteed to always be unique.
	start := time.Now()

	// Same as other one
	data := readInDataNaive()

	// I will spin up 9 go routines. 1 for each size of groups we'll be analyzing
	// Once that's done, I will print out text to tell us the dependencies
	wg := new(sync.WaitGroup)
	wg.Add(9)

	// Will run naive version of all the below
	// Only need to run up to groups of 9 since
	// we have 10 columns and the right of the functional
	// dependency always has 1
	go checkOneOnLeft(wg, data)
	go checkTwoOnLeft(wg, data)
	go checkThreeOnLeft(wg, data)
	go checkFourOnLeft(wg, data)
	go checkFiveOnLeft(wg, data)
	go checkSixOnLeft(wg, data)
	go checkSevenOnLeft(wg, data)
	go checkEightOnLeft(wg, data)
	go checkNineOnLeft(wg, data)

	wg.Wait()
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
	_ = data
}
