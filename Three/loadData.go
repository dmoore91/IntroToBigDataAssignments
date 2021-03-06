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

type role struct {
	RoleID int
	Role   string
}

type roles struct {
	Roles []role
}

type titleActorRole struct {
	Tconst         string
	Nconst         string
	RoleList       roles
	RuntimeMinutes int
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
		log.Error(err)
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

				runtimeMinutes := row[7]

				if row[7] == "" {
					runtimeMinutes = "0"
				}

				t := title{
					Id:             id,
					TitleType:      row[1],
					StartYear:      row[5],
					RuntimeMinutes: runtimeMinutes,
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
		log.Error(err)
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

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Three/Data/name.tsv")
	if err != nil {
		log.Error(err)
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

func linkTitleActorAndRoles(titles map[string]title, people map[string]person,
	titleIds map[string]int) map[int][]titleActorRole {

	file, err := os.Open("/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Three/Data/principals.tsv")
	if err != nil {
		log.Error(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	scanner.Scan()

	roleMap := make(map[string]int)
	roleNumber := 1

	titleActorRoleMap := make(map[int][]titleActorRole)

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
			if len(row) == 6 && (row[3] == "actor") {

				titleId, titleOK := titleIds[row[0]] // Get titleID from tconst
				_, personOK := people[row[2]]        // Get memberID from nconst

				if titleOK && personOK { //Have to add this because sometimes they aren't in members

					rolesList := roles{Roles: []role{}}

					rolesForActor := strings.Split(row[5], "\",\"")

					runtimeMinutes, err := strconv.Atoi(titles[row[0]].RuntimeMinutes)
					if err != nil {
						log.Error(err)
					}

					// Add roles to map if they don't exist
					// Add role and roleID to actor's list of roles
					for _, elem := range rolesForActor {

						tmp := strings.ReplaceAll(elem, "\"", "")
						tmp = strings.ReplaceAll(tmp, "]", "")
						tmp = strings.ReplaceAll(tmp, "[", "")

						//Need to escape backslashes or postgres gets mad
						tmp = strings.ReplaceAll(tmp, "\\", "\\\\")

						if tmp != "" {

							roleStruct := role{
								RoleID: 0, Role: tmp}

							roleID, ok := roleMap[tmp]
							if !ok {
								roleMap[tmp] = roleNumber
								roleNumber += 1
								roleStruct.RoleID = roleNumber
							}

							roleStruct.RoleID = roleID

							rolesList.Roles = append(rolesList.Roles, roleStruct)
						}
					}

					tar := titleActorRole{
						Tconst:         row[0],
						Nconst:         row[2],
						RoleList:       rolesList,
						RuntimeMinutes: runtimeMinutes,
					}

					titleActorRoleMap[titleId] = append(titleActorRoleMap[titleId], tar)
				}
			}
		}
	}

	return titleActorRoleMap
}

func filterTitleIds(titleIDs map[string]int, titleActorRoleMap map[int][]titleActorRole) []string {

	var validTitleTconsts []string

	var valid bool

	for _, titleID := range titleIDs {

		valid = true
		tars := titleActorRoleMap[titleID]

		if len(tars) > 0 {

			//Only interested in ones >= 90
			if tars[0].RuntimeMinutes < 90 {
				continue
			}

			for _, tar := range tars {
				if len(tar.RoleList.Roles) != 1 {
					valid = false
					break
				}
			}

			if valid {
				validTitleTconsts = append(validTitleTconsts, tars[0].Tconst)
			}
		}
	}

	return validTitleTconsts
}

func writeListOfDbEntries(validTitleTconsts []string, titles map[string]title, people map[string]person,
	genres map[string]int, titleActorRoleMap map[int][]titleActorRole) {

	file, err := os.Create("Three/entries.tsv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	w := csv.NewWriter(file)
	w.Comma = '\t'

	for _, elem := range validTitleTconsts {
		titleInfo := titles[elem]

		titleID, err := strconv.Atoi(titleInfo.Id)
		if err != nil {
			log.Fatal(err)
		}

		for _, genre := range titleInfo.Genres {

			genreID := genres[genre]

			tars := titleActorRoleMap[titleID]

			for _, tar := range tars {
				personInfo := people[tar.Nconst]

				var lines []string

				lines = append(lines, titleInfo.Id)                      //movieID
				lines = append(lines, titleInfo.TitleType)               //type
				lines = append(lines, titleInfo.StartYear)               //startYear
				lines = append(lines, titleInfo.RuntimeMinutes)          //runtime
				lines = append(lines, titleInfo.AvgRating)               //avgRating
				lines = append(lines, strconv.Itoa(genreID))             //genreID
				lines = append(lines, genre)                             //genre
				lines = append(lines, strconv.Itoa(personInfo.MemberID)) //memberID
				lines = append(lines, personInfo.BirthYear)              //birthYear
				lines = append(lines, tar.RoleList.Roles[0].Role)        //role

				err := w.Write(lines)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	w.Flush()
}

func writeEntriesToDb() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_three")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "COPY Movie_Actor_Role(movieID, type, startYear, runtimeMinutes, avgRating, genre_id, genre, " +
		"member_id, birthYear, role) " +
		"FROM '/home/dan/Documents/College/BigData/IntroToBigDataAssignments/Three/entries.tsv' " +
		"WITH (DELIMITER E'\t', NULL '');"

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

// run with go build loadData.go
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

	titleActorRoleMap := linkTitleActorAndRoles(titles, people, titleIds)

	validTitleIds := filterTitleIds(titleIds, titleActorRoleMap)

	writeListOfDbEntries(validTitleIds, titles, people, genres, titleActorRoleMap)

	writeEntriesToDb()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
