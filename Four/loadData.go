package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"time"
)

type actorStruct struct {
	Actor int
	Roles []string
}

type actorList struct {
	Actors []actorStruct
}

type title struct {
	_Id       int
	Type      string
	Title     string
	StartYear int
	EndYear   int
	Runtime   int
	AvgRating decimal.Decimal
	NumVotes  int
	Genres    []string
	Actors    actorList
	Directors []int
	Writers   []int
	Producers []int
}

type dbTitle struct {
	Id             sql.NullInt32       `json:"_id"`
	TitleType      sql.NullString      `json:"type"`
	OriginalTitle  sql.NullString      `json:"title"`
	StartYear      sql.NullInt32       `json:"startYear"`
	EndYear        sql.NullInt32       `json:"endYear"`
	RuntimeMinutes sql.NullInt32       `json:"runtime"`
	AvgRating      decimal.NullDecimal `json:"avgRating"`
	NumVotes       sql.NullInt32       `json:"numVotes"`
}

type person struct {
	_Id       int    `json:"_id"`
	Name      string `json:"name"`
	BirthYear int    `json:"birthYear"`
	DeathYear int    `json:"deathYear"`
}

type dbPerson struct {
	MemberID    sql.NullInt32  `json:"_id"`
	PrimaryName sql.NullString `json:"name"`
	BirthYear   sql.NullInt32  `json:"birthYear"`
	DeathYear   sql.NullInt32  `json:"deathYear"`
}

func getGenresForTitle() map[int][]string {

	genreMap := make(map[int][]string)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT title, Genre.genre " +
		"FROM Title_Genre " +
		"INNER JOIN Genre ON Title_Genre.genre = genre.id "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var titleID int
		var genre string

		err = rows.Scan(&titleID, &genre)
		if err != nil {
			log.Fatal(err)
		}

		genreMap[titleID] = append(genreMap[titleID], genre)
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	return genreMap
}

func getDirectorsForTitle() map[int][]int {

	directorsMap := make(map[int][]int)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT director " +
		"FROM Title_Director "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var titleID int
		var director int

		err = rows.Scan(&director)
		if err != nil {
			log.Fatal(err)
		}

		directorsMap[titleID] = append(directorsMap[titleID], director)
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	return directorsMap
}

func getWritersForTitle() map[int][]int {

	writerMap := make(map[int][]int)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT title, writer " +
		"FROM Title_Writer "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var titleID int
		var writer int

		err = rows.Scan(&titleID, &writer)
		if err != nil {
			log.Fatal(err)
		}

		writerMap[titleID] = append(writerMap[titleID], writer)
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	return writerMap
}

func getProducersForTitle() map[int][]int {

	producerMap := make(map[int][]int)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT title, producer " +
		"FROM Title_Producer "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var titleID int
		var producer int

		err = rows.Scan(&titleID, &producer)
		if err != nil {
			log.Fatal(err)
		}

		producerMap[titleID] = append(producerMap[titleID], producer)
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	return producerMap
}

func getRolesForActorForTitle() map[int]map[int][]string {

	rolesMap := make(map[int]map[int][]string)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT title, actor, Role.role " +
		"FROM Actor_Title_Role " +
		"INNER JOIN Role on Role.id = Actor_Title_Role.role "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var titleID int
		var actorID int
		var role string

		err = rows.Scan(&titleID, &actorID, &role)
		if err != nil {
			log.Fatal(err)
		}

		_, ok := rolesMap[titleID][actorID]

		if ok {
			rolesMap[titleID][actorID] = append(rolesMap[titleID][actorID], role)
		} else {
			rolesMap[titleID] = make(map[int][]string)
			rolesMap[titleID][actorID] = append(rolesMap[titleID][actorID], role)
		}
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	return rolesMap
}

func getActorsForTitle() map[int]actorList {

	rolesMap := getRolesForActorForTitle()

	titleActorMap := make(map[int]actorList)

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT title, actor " +
		"FROM Title_Actor"
	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var actorID int
		var titleID int

		err = rows.Scan(&titleID, &actorID)
		if err != nil {
			log.Fatal(err)
		}

		a := actorStruct{
			Actor: actorID,
			Roles: rolesMap[titleID][actorID],
		}

		l := titleActorMap[titleID].Actors

		l = append(l, a)

		titleActorMap[titleID] = actorList{Actors: l}
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

	return titleActorMap
}

func readTitleTable() []title {

	var titleList []title

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close(context.Background())

	queryString := "SELECT id, type, originalTitle, startYear, endYear, runtimeminutes, avgrating, numvotes " +
		"FROM Title "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var db dbTitle

		err = rows.Scan(&db.Id, &db.TitleType, &db.OriginalTitle, &db.StartYear, &db.EndYear, &db.RuntimeMinutes,
			&db.AvgRating, &db.NumVotes)
		if err != nil {
			log.Fatal(err)
		}

		var t title

		if db.Id.Valid {
			t._Id = int(db.Id.Int32)
		}

		if db.TitleType.Valid {
			t.Type = db.TitleType.String
		}

		if db.OriginalTitle.Valid {
			t.Title = db.OriginalTitle.String
		}

		if db.StartYear.Valid {
			t.StartYear = int(db.StartYear.Int32)
		}

		if db.EndYear.Valid {
			t.EndYear = int(db.EndYear.Int32)
		}

		if db.RuntimeMinutes.Valid {
			t.Runtime = int(db.RuntimeMinutes.Int32)
		}

		if db.AvgRating.Valid {
			t.AvgRating = db.AvgRating.Decimal
		}

		if db.NumVotes.Valid {
			t.NumVotes = int(db.NumVotes.Int32)
		}

		titleList = append(titleList, t)
	}

	return titleList
}

func addMovies(wg *sync.WaitGroup) {

	defer wg.Done()

	titles := readTitleTable()
	fmt.Println("Got Titles")

	producerMap := getProducersForTitle()
	fmt.Println("Got Producers")

	writerMap := getWritersForTitle()
	fmt.Println("Got Writers")

	directorMap := getDirectorsForTitle()
	fmt.Println("Got Directors")

	genreMap := getGenresForTitle()
	fmt.Println("Got Genres")

	titleActorMap := getActorsForTitle()
	fmt.Println("Got Actors")

	client := connectToMongo()

	for _, title := range titles {
		title.Genres = genreMap[title._Id]
		title.Writers = writerMap[title._Id]
		title.Producers = producerMap[title._Id]
		title.Actors = titleActorMap[title._Id]
		title.Directors = directorMap[title._Id]

		client.Database("assignment_four").Collection("Movies").InsertOne(context.Background(), title)
	}
}

func getNamesMap(wg *sync.WaitGroup) {

	defer wg.Done()

	client := connectToMongo()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT id, name, birthYear, deathYear " +
		"FROM Member "

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {

		var db dbPerson

		err = rows.Scan(&db.MemberID, &db.PrimaryName, &db.BirthYear, &db.DeathYear)
		if err != nil {
			log.Fatal(err)
		}

		var p person

		if db.MemberID.Valid {
			p._Id = int(db.MemberID.Int32)
		}

		if db.PrimaryName.Valid {
			p.Name = db.PrimaryName.String
		}

		if db.BirthYear.Valid {
			p.BirthYear = int(db.BirthYear.Int32)
		}

		if db.DeathYear.Valid {
			p.DeathYear = int(db.DeathYear.Int32)
		}

		_, err = client.Database("assignment_four").Collection("Members").InsertOne(context.Background(), p)

		if err != nil {
			log.Fatal(err)
		}
	}

	err = conn.Close(context.Background())

	if err != nil {
		log.Fatal(err)
	}

}

func connectToMongo() *mongo.Client {
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

	wg.Add(2)

	go addMovies(wg)
	go getNamesMap(wg)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
