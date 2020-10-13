package main

import (
	"bufio"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type title struct {
	Id             string
	TitleType      string
	OriginalTitle  string
	StartYear      string
	EndYear        string
	RuntimeMinutes string
	AvgRating      string
	NumVotes       string
}

type person struct {
	MemberID    int
	PrimaryName string
	BirthYear   string
	DeathYear   string
}

func (t title) ToTSVString() string {
	builder := strings.Builder{}

	builder.WriteString(t.Id)
	builder.WriteString("\t")
	builder.WriteString(t.TitleType)
	builder.WriteString("\t")
	builder.WriteString(t.OriginalTitle)
	builder.WriteString("\t")
	builder.WriteString(t.StartYear)
	builder.WriteString("\t")
	builder.WriteString(t.EndYear)
	builder.WriteString("\t")
	builder.WriteString(t.RuntimeMinutes)
	builder.WriteString("\t")
	builder.WriteString(t.AvgRating)
	builder.WriteString("\t")
	builder.WriteString(t.NumVotes)

	return builder.String()
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
				t.AvgRating = row[1]
				t.NumVotes = row[2]

				m[row[0]] = t
			}
		}
	}
	return m
}

func readInTitles(m map[string]title) (map[string]title, map[string]int) {

	titleIds := make(map[string]int)

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

				id := strconv.Itoa(idx)

				titleIds[row[0]] = idx

				t := title{
					Id:             id,
					TitleType:      row[1],
					OriginalTitle:  row[3],
					StartYear:      row[5],
					EndYear:        row[6],
					RuntimeMinutes: row[7],
				}

				m[row[0]] = t

				idx += 1
			}
		}
	}

	return m, titleIds
}

func populateTitleTable(wg *sync.WaitGroup, titleIdsChan chan map[string]int) {

	defer wg.Done()

	titles := make(map[string]title)

	titles, titleIds := readInTitles(titles)
	titles = readInRatings(titles)

	titleIdsChan <- titleIds
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

func readInCrewTSV(wg *sync.WaitGroup, people map[string]person, titleIds map[string]int) {

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

				titleId, titleOk := titleIds[row[0]] // Get titleID from tconst

				if titleOk {

					directors := strings.Split(row[1], ",")
					writers := strings.Split(row[2], ",")

					_ = titleId
					_ = directors
					_ = writers

				}
			}
		}
	}
}

func readInActorsAndProducers(wg *sync.WaitGroup, people map[string]person, titleIds map[string]int) {

	defer wg.Done()

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Four/Data/principals.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	roleMap := make(map[string]int)
	roleNumber := 1

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

				titleId, titleOK := titleIds[row[0]] // Get titleID from tconst
				p, personOK := people[row[2]]        // Get memberID from nconst

				_ = p
				_ = titleId

				if titleOK && personOK { //Have to add this because sometimes they aren't im members

					roles := strings.Split(row[5], "\",\"")

					//Add roles to map if they don't exist
					for _, elem := range roles {

						tmp := strings.ReplaceAll(elem, "\"", "")
						tmp = strings.ReplaceAll(tmp, "]", "")
						tmp = strings.ReplaceAll(tmp, "[", "")

						//Need to escape backslashes or postgres gets mad
						tmp = strings.ReplaceAll(tmp, "\\", "\\\\")

						_, ok := roleMap[tmp]
						if !ok {
							roleMap[tmp] = roleNumber
							roleNumber += 1
						}

						//At this point we are guaranteed to have role ids
						//Map lookup is O(1) so doing it twice isn't a big deal

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

	//titleIdsChan := make(chan map[string]int)
	peopleChan := make(chan map[string]person)

	wg.Add(1)

	//go populateTitleTable(wg, titleIdsChan)
	go getNamesMap(wg, peopleChan)

	//titleIds := <-titleIdsChan
	people := <-peopleChan

	wg.Wait()

	//_ = titleIds
	_ = people

	//wg.Add(2)
	//
	//go readInCrewTSV(wg, people, titleIds)
	//go readInActorsAndProducers(wg, people, titleIds)
	//
	//wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
