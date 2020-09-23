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

func populateTitleTable() map[string]int {

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

func getNamesMap() map[string]person {

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

				line = append(line, strconv.Itoa(p.MemberID))
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

func addWritersToDB() {
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

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func addDirectorsToDB() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title_Director FROM '/home/danielmoore/Documents/College/BigData/Two/title_director.csv' " +
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

func readInCrewTSV(wg *sync.WaitGroup, people map[string]person, titleIds map[string]int) {

	defer wg.Done()

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

				titleId, titleOk := titleIds[row[0]] // Get titleID from tconst

				if titleOk {

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
	}

	writerWriter.Flush()
	dWriter.Flush()

	addWritersToDB()
	addDirectorsToDB()

}

func addRolesToDatabase(roleMap map[string]int) {

	roleFile, err := os.Create("Two/roles.tsv")
	if err != nil {
		log.Fatal(err)
	}
	defer roleFile.Close()

	roleWriter := csv.NewWriter(roleFile)
	roleWriter.Comma = '\t'

	for role, roleID := range roleMap {
		var lines []string

		lines = append(lines, strconv.Itoa(roleID))
		lines = append(lines, role)

		if role != "" && len(role) != 0 {
			err := roleWriter.Write(lines)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	roleWriter.Flush()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Role FROM '/home/danielmoore/Documents/College/BigData/Two/roles.tsv' " +
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

func addProducersToDatabase() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title_Producer FROM '/home/danielmoore/Documents/College/BigData/Two/title_producer.csv' " +
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

func addActorsToDatabase() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Title_Actor FROM '/home/danielmoore/Documents/College/BigData/Two/title_actor.csv' " +
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

func addActorTitleRoleToDB() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Actor_Title_Role FROM '/home/danielmoore/Documents/College/BigData/Two/actorTitleRole.csv' " +
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

func readInActorsAndProducers(wg *sync.WaitGroup, people map[string]person, titleIds map[string]int) {

	defer wg.Done()

	file, err := os.Open("/home/danielmoore/Documents/College/BigData/Two/data/principals.tsv")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	roleMap := make(map[string]int)
	roleNumber := 1

	actorFile, err := os.Create("Two/title_actor.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer actorFile.Close()

	actorWriter := csv.NewWriter(actorFile)

	producerFile, err := os.Create("Two/title_producer.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer producerFile.Close()

	producerWriter := csv.NewWriter(producerFile)

	actorTitleRoleFile, err := os.Create("Two/actorTitleRole.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer producerFile.Close()

	actorTitleRoleWriter := csv.NewWriter(actorTitleRoleFile)

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

				if titleOK && personOK { //Have to add this because sometimes they aren't im members

					tmp := strings.ReplaceAll(row[5], "[", "")
					tmp = strings.ReplaceAll(tmp, "]", "")

					roles := []string{tmp}

					//Add roles to map if they don't exist
					for _, elem := range roles {

						_, ok := roleMap[elem]
						if !ok {
							roleMap[elem] = roleNumber
							roleNumber += 1
						}

						//At this point we are guaranteed to have role ids
						//Map lookup is O(1) so doing it twice isn't a big deal
						if row[3] == "actor" {
							var actorTitleRoleLines []string
							actorTitleRoleLines = append(actorTitleRoleLines, strconv.Itoa(p.MemberID))
							actorTitleRoleLines = append(actorTitleRoleLines, strconv.Itoa(titleId))
							actorTitleRoleLines = append(actorTitleRoleLines, strconv.Itoa(roleMap[elem]))

							err := actorTitleRoleWriter.Write(actorTitleRoleLines)
							if err != nil {
								log.Fatal(err)
							}
						}
					}

					if row[3] == "actor" {
						var actorLines []string
						actorLines = append(actorLines, strconv.Itoa(p.MemberID))
						actorLines = append(actorLines, strconv.Itoa(titleId))

						err := actorWriter.Write(actorLines)
						if err != nil {
							log.Fatal(err)
						}
					} else {
						var producerLines []string
						producerLines = append(producerLines, strconv.Itoa(p.MemberID))
						producerLines = append(producerLines, strconv.Itoa(titleId))

						err := producerWriter.Write(producerLines)
						if err != nil {
							log.Fatal(err)
						}
					}
				}
			}
		}
	}

	actorWriter.Flush()
	producerWriter.Flush()
	actorTitleRoleWriter.Flush()

	//Add role and actors/producer to database
	addRolesToDatabase(roleMap)
	addActorsToDatabase()
	addProducersToDatabase()

	//Add Actor_Title_Role data now that database is prepped
	addActorTitleRoleToDB()
}

func main() {

	start := time.Now()

	titleIds := populateTitleTable()
	people := getNamesMap()

	wg := new(sync.WaitGroup)
	wg.Add(2)

	readInCrewTSV(wg, people, titleIds)
	readInActorsAndProducers(wg, people, titleIds)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
