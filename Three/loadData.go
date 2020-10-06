package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type title struct {
	Id             string
	TitleType      string
	StartYear      string
	RuntimeMinutes string
	AvgRating      string
	Genres         []string
}

type person struct {
	MemberID    int
	PrimaryName string
	BirthYear   string
	DeathYear   string
}

//Adds to genre map given genres from title
func addToGenreMap(genres map[string]int, genreList []string, genreNumber int) (map[string]int, int) {

	for _, elem := range genreList {
		_, ok := genres[elem]
		if !ok {
			genres[elem] = genreNumber
			genreNumber += 1
		}
	}

	return genres, genreNumber
}

//Having method return genres so when writing we can make an entry for each genre of each title
func readInTitles(m map[string]title) (map[string]title, map[string]int, map[string]int) {

	titleIds := make(map[string]int)

	genres := make(map[string]int)

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Three/Data/title.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	idx := 1
	genreNumber := 1

	for scanner.Scan() {
		txt := scanner.Text()

		if !strings.Contains(txt, "startyear") && txt != "" {
			i := strings.Index(txt, "\\N")

			for {
				if i == -1 {
					break
				}

				txt = txt[:i] + txt[i+2:]
				i = strings.Index(txt, "\\N")
			}

			row := strings.Split(txt, "\t")
			if len(row) == 9 {

				id := strconv.Itoa(idx)

				titleIds[row[0]] = idx

				t := title{
					Id:             id,
					TitleType:      row[1],
					StartYear:      row[5],
					RuntimeMinutes: row[7],
					Genres:         strings.Split(row[8], ","),
				}

				m[row[0]] = t

				idx += 1

				genres, genreNumber = addToGenreMap(genres, strings.Split(row[8], ","), genreNumber)
			}
		}
	}

	return m, titleIds, genres
}

func readInRatings(m map[string]title) map[string]title {

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Three/Data/ratings.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		txt := scanner.Text()

		i := strings.Index(txt, "\\N")

		for {
			if i == -1 {
				break
			}

			txt = txt[:i] + txt[i+2:]
			i = strings.Index(txt, "\\N")
		}

		if !strings.Contains(txt, "averageRating") {
			row := strings.Split(txt, "\t")
			if len(row) == 3 {

				t := m[row[0]]
				t.AvgRating = row[1]

				m[row[0]] = t
			}
		}
	}

	return m
}

func populateTitleTable(wg *sync.WaitGroup, titleChan chan map[string]title, titleIdsChan chan map[string]int,
	genresChan chan map[string]int) {

	defer wg.Done()

	titles := make(map[string]title)

	titles, titleIds, genres := readInTitles(titles)
	titles = readInRatings(titles)

	titleChan <- titles
	titleIdsChan <- titleIds
	genresChan <- genres
}

func getNamesMap(wg *sync.WaitGroup, peopleChan chan map[string]person) {

	defer wg.Done()

	people := make(map[string]person)

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Two/Data/name.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	idx := 1

	for scanner.Scan() {
		txt := scanner.Text()

		if txt != "" {
			i := strings.Index(txt, "\\N")

			for {
				if i == -1 {
					break
				}

				txt = txt[:i] + txt[i+2:]
				i = strings.Index(txt, "\\N")
			}

			row := strings.Split(txt, "\t")
			if len(row) == 6 {

				p := person{
					MemberID:    idx,
					PrimaryName: row[1],
					BirthYear:   row[2],
					DeathYear:   row[3],
				}

				people[row[0]] = p
			}
		}

		idx += 1
	}

	peopleChan <- people
}

func main() {

	start := time.Now()

	titleIdsChan := make(chan map[string]int)
	genresChan := make(chan map[string]int)
	titlesChan := make(chan map[string]title)
	peopleChan := make(chan map[string]person)

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go populateTitleTable(wg, titlesChan, titleIdsChan, genresChan)
	go getNamesMap(wg, peopleChan)

	titles := <-titlesChan
	titleIds := <-titleIdsChan
	genres := <-genresChan
	people := <-peopleChan

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)

	//Just set to nothing to get rid of error
	_ = titles
	_ = titleIds
	_ = genres
	_ = people
}
