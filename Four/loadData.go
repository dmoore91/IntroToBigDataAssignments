package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type actor struct {
	ActorId int      `json:"actor"`
	Roles   []string `json:"roles"`
}

type actorList struct {
	Actors []actor
}

type title struct {
	Id             int             `json:"_id"`
	TitleType      string          `json:"type"`
	OriginalTitle  string          `json:"title"`
	StartYear      int             `json:"startYear"`
	EndYear        int             `json:"endYear"`
	RuntimeMinutes int             `json:"runtime"`
	AvgRating      decimal.Decimal `json:"avgRating"`
	NumVotes       int             `json:"numVotes"`
	Genres         []string        `json:"genres"`
	Actors         actorList       `json:"actors"`
	Directors      []int           `json:"directors"`
	Writers        []int           `json:"writers"`
	Producers      []int           `json:"producer"`
}

type person struct {
	MemberID    int    `json:"_id"`
	PrimaryName string `json:"name"`
	BirthYear   string `json:"birthYear"`
	DeathYear   string `json:"deathYear"`
}

//Reads ratings file into graviton db
func readInRatings(m map[string]title) map[string]title {

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Four/Data/ratings.tsv")
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

				tmp, err := decimal.NewFromString(row[1])

				if err == nil {
					t.AvgRating = tmp
				}

				intTmp, err := strconv.Atoi(row[2])

				if err == nil {
					t.NumVotes = intTmp
				}

				m[row[0]] = t
			}
		}
	}
	return m
}

func readInTitles(m map[string]title) map[string]title {

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Four/Data/title.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	idx := 1

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

				t := title{
					Id:            idx,
					TitleType:     row[1],
					OriginalTitle: row[3],
					Genres:        strings.Split(row[8], ","),
				}

				tmp, err := strconv.Atoi(row[5])

				if err == nil {
					t.StartYear = tmp
				}

				tmp, err = strconv.Atoi(row[6])

				if err == nil {
					t.EndYear = tmp
				}

				tmp, err = strconv.Atoi(row[7])

				if err == nil {
					t.RuntimeMinutes = tmp
				}

				m[row[0]] = t

				idx += 1
			}
		}
	}

	return m
}

func populateTitleTable(wg *sync.WaitGroup, titleChan chan map[string]title) {

	defer wg.Done()

	titles := make(map[string]title)

	titles = readInTitles(titles)
	titles = readInRatings(titles)

	titleChan <- titles
}

func getNamesMap(wg *sync.WaitGroup, peopleChan chan map[string]person) {

	defer wg.Done()

	people := make(map[string]person)

	client := ConnectToDatabase()

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Four/Data/name.tsv")
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

				_, err = client.Database("assignment_four").Collection("Members").InsertOne(context.Background(), p)

				if err != nil {
					log.Error(err)
				}

				people[row[0]] = p
			}
		}
		idx += 1
	}
	peopleChan <- people
}

func readInCrewTSV(wg *sync.WaitGroup, people map[string]person, titles map[string]title) {

	defer wg.Done()

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Four/Data/crew.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

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
			if len(row) == 3 {

				title, titleOk := titles[row[0]] // Get titleID from tconst

				if titleOk {

					directors := strings.Split(row[1], ",")
					writers := strings.Split(row[2], ",")

					var directorsArr []int

					for _, elem := range directors {
						directorsArr = append(directorsArr, people[elem].MemberID)
					}

					var writersArr []int

					for _, elem := range writers {
						writersArr = append(writersArr, people[elem].MemberID)
					}

					title.Directors = directorsArr
					title.Writers = writersArr
				}
			}
		}
	}
}

func readInActorsAndProducers(wg *sync.WaitGroup, people map[string]person, titles map[string]title) {

	defer wg.Done()

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Four/Data/principals.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

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
			if len(row) == 6 && (row[3] == "producer" || row[3] == "actor") {

				title, titleOK := titles[row[0]] // Get titleID from tconst
				p, personOK := people[row[2]]    // Get memberID from nconst

				if titleOK && personOK {

					if row[3] == "producer" {
						title.Producers = append(title.Producers, p.MemberID)
					} else if row[3] == "actor" {
						a := actor{
							ActorId: p.MemberID,
							Roles:   strings.Split(row[5], ","),
						}

						title.Actors.Actors = append(title.Actors.Actors, a)
					}
				}
			}
		}
	}
}

func ConnectToDatabase() *mongo.Client {
	// Set client options
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)

	if err != nil {
		log.Fatal(err)
	}

	return client
}

func main() {

	start := time.Now()

	wg := new(sync.WaitGroup)

	titlesChan := make(chan map[string]title)
	peopleChan := make(chan map[string]person)

	wg.Add(2)

	go populateTitleTable(wg, titlesChan)
	go getNamesMap(wg, peopleChan)

	titles := <-titlesChan
	people := <-peopleChan

	wg.Wait()

	wg.Add(2)

	go readInCrewTSV(wg, people, titles)
	go readInActorsAndProducers(wg, people, titles)

	wg.Wait()

	client := ConnectToDatabase()

	for _, elem := range titles {
		_, err := client.Database("assignment_four").Collection("Movies").InsertOne(context.Background(), elem)

		if err != nil {
			log.Error(err)
		}
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
