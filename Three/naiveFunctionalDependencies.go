package main

import (
	"fmt"
	"sync"
	"time"
)

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
