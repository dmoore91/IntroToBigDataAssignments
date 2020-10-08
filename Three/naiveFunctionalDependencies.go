package main

import (
	"database/sql"
	"fmt"
	"github.com/shopspring/decimal"
	"sync"
	"time"
)

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

func verifyInTitleIdMap(bytes []byte, m map[string]int, curr int) bool {

}

func checkMovieIDNaive(wg *sync.WaitGroup, data []movieTitleActor) {

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
	isValid := []bool{true, true, true, true, true, true, true, true, true, true}

	for _, elem := range data {

		valid := verifyInTitleIdMap

	}

	header := ""

	for idx, valid := range isValid {

		if valid {
			switch idx {
			case 0:
				header += "movieID->"
			}
		}
	}

	println(header + "movieID")
}

func main() {
	start := time.Now()

	// Same as other one
	data := readInData()

	// I will spin up 10 go routines. 1 for each column
	// I will then iterate through the list of data, keep track of the mappings and if they change, I will
	// store that that dependency doesn't match in a 9 element boolean array.
	// Once that's done, I will print out text to tell us the dependencies

	wg := new(sync.WaitGroup)

	wg.Add(10)

	// Will run naive version of all the below
	//go checkMovieID(wg, data)
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
