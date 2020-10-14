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
	"strconv"
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
	MemberID    int    `json:"_id"`
	PrimaryName string `json:"name"`
	BirthYear   int    `json:"birthYear"`
	DeathYear   int    `json:"deathYear"`
}

type dbPerson struct {
	MemberID    sql.NullInt32  `json:"_id"`
	PrimaryName sql.NullString `json:"name"`
	BirthYear   sql.NullString `json:"birthYear"`
	DeathYear   sql.NullString `json:"deathYear"`
}

func getGenresForTitle(titleId int, conn *pgx.Conn) []string {

	queryString := "SELECT Genre.genre " +
		"FROM Title_Genre " +
		"INNER JOIN Genre ON Title_Genre.genre = genre.id " +
		"WHERE Title_Genre.title=$1;"

	rows, err := conn.Query(context.Background(), queryString, titleId)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var genres []string

	for rows.Next() {

		var genre string

		err = rows.Scan(&genre)
		if err != nil {
			log.Fatal(err)
		}

		genres = append(genres, genre)
	}

	return genres
}

func getDirectorsForTitle(titleId int, conn *pgx.Conn) []int {

	queryString := "SELECT director " +
		"FROM Title_Director " +
		"WHERE title=$1;"

	rows, err := conn.Query(context.Background(), queryString, titleId)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var directors []int

	for rows.Next() {

		var director int

		err = rows.Scan(&director)
		if err != nil {
			log.Fatal(err)
		}

		directors = append(directors, director)
	}

	return directors
}

func getWritersForTitle(titleId int, conn *pgx.Conn) []int {

	queryString := "SELECT writer " +
		"FROM Title_Writer " +
		"WHERE title=$1;"

	rows, err := conn.Query(context.Background(), queryString, titleId)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var writers []int

	for rows.Next() {

		var writer int

		err = rows.Scan(&writer)
		if err != nil {
			log.Fatal(err)
		}

		writers = append(writers, writer)
	}

	return writers
}

func getProducersForTitle(titleId int, conn *pgx.Conn) []int {

	queryString := "SELECT producer " +
		"FROM Title_Producer " +
		"WHERE title=$1;"

	rows, err := conn.Query(context.Background(), queryString, titleId)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var producers []int

	for rows.Next() {

		var producer int

		err = rows.Scan(&producer)
		if err != nil {
			log.Fatal(err)
		}

		producers = append(producers, producer)
	}

	return producers
}

func getRolesForActorForTitle(titleId int, actorID int, conn *pgx.Conn) actor {

	queryString := "SELECT Role.role " +
		"FROM Actor_Title_Role " +
		"INNER JOIN Role on Role.id = Actor_Title_Role.role " +
		"WHERE title=$1 AND actor=$2"

	rows, err := conn.Query(context.Background(), queryString, titleId, actorID)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var roles []string

	for rows.Next() {

		var role string

		err = rows.Scan(&role)
		if err != nil {
			log.Fatal(err)
		}

		roles = append(roles, role)
	}

	return actor{
		ActorId: actorID,
		Roles:   roles,
	}
}

func getActorsForTitle(titleId int, conn *pgx.Conn) actorList {

	queryString := "SELECT actor " +
		"FROM Title_Actor " +
		"WHERE title=$1;"

	rows, err := conn.Query(context.Background(), queryString, titleId)

	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	var a actorList

	for rows.Next() {

		var actorID int

		err = rows.Scan(&actorID)
		if err != nil {
			log.Fatal(err)
		}

		a.Actors = append(a.Actors, getRolesForActorForTitle(titleId, actorID, conn))
	}

	return a
}

func populateTitleTable(wg *sync.WaitGroup) {

	defer wg.Done()

	client := ConnectToMongo()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

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
			t.Id = int(db.Id.Int32)
		}

		if db.TitleType.Valid {
			t.TitleType = db.TitleType.String
		}

		if db.OriginalTitle.Valid {
			t.OriginalTitle = db.OriginalTitle.String
		}

		if db.StartYear.Valid {
			t.StartYear = int(db.StartYear.Int32)
		}

		if db.EndYear.Valid {
			t.EndYear = int(db.EndYear.Int32)
		}

		if db.RuntimeMinutes.Valid {
			t.RuntimeMinutes = int(db.RuntimeMinutes.Int32)
		}

		if db.AvgRating.Valid {
			t.AvgRating = db.AvgRating.Decimal
		}

		if db.NumVotes.Valid {
			t.NumVotes = int(db.NumVotes.Int32)
		}

		t.Genres = getGenresForTitle(t.Id, conn)
		t.Directors = getDirectorsForTitle(t.Id, conn)
		t.Writers = getWritersForTitle(t.Id, conn)
		t.Producers = getProducersForTitle(t.Id, conn)
		t.Actors = getActorsForTitle(t.Id, conn)

		_, err = client.Database("assignment_four").Collection("Movies").InsertOne(context.Background(), t)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func getNamesMap(wg *sync.WaitGroup) {

	defer wg.Done()

	client := ConnectToMongo()

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

		err = rows.Scan(&db.MemberID, &db.PrimaryName, db.BirthYear, db.DeathYear)
		if err != nil {
			log.Fatal(err)
		}

		var p person

		if db.MemberID.Valid {
			p.MemberID = int(db.MemberID.Int32)
		}

		if db.PrimaryName.Valid {
			p.PrimaryName = db.PrimaryName.String
		}

		if db.BirthYear.Valid {
			p.BirthYear, err = strconv.Atoi(db.BirthYear.String)
			if err != nil {
				log.Fatal(err)
			}
		}

		if db.DeathYear.Valid {
			p.DeathYear, err = strconv.Atoi(db.DeathYear.String)
			if err != nil {
				log.Fatal(err)
			}
		}

		client.Database("assignment_four").Collection("Members").InsertOne(context.Background(), p)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func ConnectToMongo() *mongo.Client {
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

	go populateTitleTable(wg)
	go getNamesMap(wg)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
