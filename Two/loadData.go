package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
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

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/ratings.tsv")
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

func processGenres(genres map[string]int, genreList []string, titleID string, w *csv.Writer, genreNumber int) (map[string]int, int) {

	for _, elem := range genreList {
		genreID, ok := genres[elem]
		if !ok {
			genres[elem] = genreNumber
			genreID = genreNumber
			genreNumber += 1
		}

		var line []string

		line = append(line, titleID)
		line = append(line, strconv.Itoa(genreID))

		err := w.Write(line)
		if err != nil {
			log.Fatal(err)
		}
	}

	return genres, genreNumber
}

func readGenresIntoDB(genres map[string]int) {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	for genre, id := range genres {
		queryString := "INSERT INTO Genre(id, genre) " +
			"VALUES($1, $2)"

		commandTag, err := conn.Exec(context.Background(), queryString, id, genre)

		if err != nil {
			log.Fatal(err)
		}

		if commandTag.RowsAffected() == 0 {
			log.Fatal(err)
		}
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

//Reads titles file into graviton db
func readInTitles(m map[string]title) (map[string]title, map[string]int) {

	titleIds := make(map[string]int)

	genres := make(map[string]int)

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/title.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	idx := 1
	genreNumber := 1

	genreFile, err := os.Create("Two/genre.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer genreFile.Close()

	w := csv.NewWriter(genreFile)
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

				genres, genreNumber = processGenres(genres, strings.Split(row[8], ","), t.Id, w, genreNumber)
			}
		}
	}

	//Make sure all line get written to file
	w.Flush()

	readGenresIntoDB(genres)

	return m, titleIds
}

//Iterates through all elements in db and
func addTitlesToDb(m map[string]title) {

	file, err := os.Create("Two/result.tsv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for _, t := range m {
		_, err := file.WriteString(t.ToTSVString())
		if err != nil {
			log.Fatal()
		}

		_, err = file.WriteString("\n")
		if err != nil {
			log.Fatal()
		}
	}

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title FROM '/home/danielmoore/Documents/College/BigData/Two/result.tsv' " +
		"WITH (DELIMITER E'\\t', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func addGenreToTableLink() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title_Genre FROM '/home/danielmoore/Documents/College/BigData/Two/genre.csv' " +
		"WITH (DELIMITER ',', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func populateTitleTable(wg *sync.WaitGroup) map[string]int {

	defer wg.Done()

	titles := make(map[string]title)

	titles, titleIds := readInTitles(titles)
	titles = readInRatings(titles)
	addTitlesToDb(titles)
	addGenreToTableLink()

	return titleIds
}

func addMembersToDB() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Member FROM '/home/danielmoore/Documents/College/BigData/Two/member.tsv' " +
		"WITH (DELIMITER E'\\t', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func getNamesMap(wg *sync.WaitGroup) map[string]person {

	defer wg.Done()

	people := make(map[string]person)

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/name.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()
	idx := 1

	file, err = os.Create("Two/member.tsv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)
	w.Comma = '\t'
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

				var line []string

				line = append(line, strconv.Itoa(idx))
				line = append(line, p.PrimaryName)
				line = append(line, p.BirthYear)
				line = append(line, p.DeathYear)

				err := w.Write(line)
				if err != nil {
					log.Fatal(err)
				}
			}
		}

		idx += 1
	}

	w.Flush()

	addMembersToDB()

	return people
}

func readInCrewTSV(people map[string]person, titleIds map[string]int) {

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/crew.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	wFile, err := os.Create("Two/title_writer.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer wFile.Close()

	writerWriter := csv.NewWriter(wFile)

	dFile, err := os.Create("Two/title_director.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer dFile.Close()

	dWriter := csv.NewWriter(dFile)
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

				titleId := titleIds[row[0]] // Get titleID from tconst

				directors := strings.Split(row[1], ",")

				for _, elem := range directors {
					var lines []string

					p, ok := people[elem]

					// Have to add this part since sometimes they aren't in members
					if ok {
						lines = append(lines, strconv.Itoa(p.MemberID))
						lines = append(lines, strconv.Itoa(titleId))

						err := dWriter.Write(lines)
						if err != nil {
							log.Fatal(err)
						}
					}
				}

				writers := strings.Split(row[2], ",")

				for _, elem := range writers {
					var lines []string

					p, ok := people[elem]

					// Have to add this part since sometimes they aren't in members
					if ok {
						lines = append(lines, strconv.Itoa(p.MemberID))
						lines = append(lines, strconv.Itoa(titleId))

						err := writerWriter.Write(lines)
						if err != nil {
							log.Fatal(err)
						}
					}
				}
			}
		}

	}

	writerWriter.Flush()
	dWriter.Flush()

	addDirectorsAndWritersToDB()
}

func addDirectorsAndWritersToDB() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title_Writer FROM '/home/danielmoore/Documents/College/BigData/Two/title_writer.csv' " +
		"WITH (DELIMITER ',', NULL '');"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}

	queryString = "COPY Title_Director FROM '/home/danielmoore/Documents/College/BigData/Two/title_director.csv' " +
		"WITH (DELIMITER ',', NULL '');"

	commandTag, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	start := time.Now()

	wg := new(sync.WaitGroup)

	//These two can be ran independently
	wg.Add(2)
	titleIds := populateTitleTable(wg)
	people := getNamesMap(wg)

	wg.Wait()

	readInCrewTSV(people, titleIds)

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
