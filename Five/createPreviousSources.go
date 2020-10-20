package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func createComedyMovieNMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW ComedyMovie AS " +
		"SELECT Title.id, Title.title, startYear FROM Title " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title.id " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE runtimeMinutes >= 75 AND Genre.genre LIKE 'Comedy';"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createNonComedyMovieNMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW NonComedyMovie AS " +
		"SELECT Title.id, Title.title, startYear FROM Title " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title.id " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE runtimeMinutes >= 75 AND Genre.genre NOT LIKE 'Comedy';"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createComedyActorNMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW ComedyActor AS " +
		"SELECT Member.id, name, birthYear, deathYear FROM Member " +
		"INNER JOIN Title_Actor ON Title_Actor.actor = Member.id " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title_Actor.title " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE Genre.genre LIKE 'Comedy';"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createNonComedyActorNMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW NonComedyActor AS " +
		"(SELECT Member.id, name, birthYear, deathYear FROM Member) " +
		"EXCEPT" +
		"(SELECT Member.id, name, birthYear, deathYear " +
		"FROM Member " +
		"INNER JOIN Title_Actor ON Title_Actor.actor = Member.id " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title_Actor.title " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE Genre.genre LIKE 'Comedy')"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createActedInNMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW ActedIn AS " +
		"SELECT actor, title FROM Title_Actor;"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createComedyMovieMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE MATERIALIZED VIEW ComedyMovieMaterialized AS " +
		"SELECT Title.id, Title.title, startYear FROM Title " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title.id " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE runtimeMinutes >= 75 AND Genre.genre LIKE 'Comedy';"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createNonComedyMovieMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE MATERIALIZED VIEW NonComedyMovieMaterialized AS " +
		"SELECT Title.id, Title.title, startYear FROM Title " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title.id " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE runtimeMinutes >= 75 AND Genre.genre NOT LIKE 'Comedy';"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createComedyActorMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE MATERIALIZED VIEW ComedyActorMaterialized AS " +
		"SELECT Member.id, name, birthYear, deathYear FROM Member " +
		"INNER JOIN Title_Actor ON Title_Actor.actor = Member.id " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title_Actor.title " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE Genre.genre LIKE 'Comedy';"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createNonComedyActorMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE MATERIALIZED VIEW NonComedyActorMaterialized AS " +
		"(SELECT Member.id, name, birthYear, deathYear FROM Member) " +
		"EXCEPT" +
		"(SELECT Member.id, name, birthYear, deathYear " +
		"FROM Member " +
		"INNER JOIN Title_Actor ON Title_Actor.actor = Member.id " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title_Actor.title " +
		"INNER JOIN Genre ON Genre.id = Title_Genre.genre " +
		"WHERE Genre.genre LIKE 'Comedy')"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func createActedInMView(wg *sync.WaitGroup) {

	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE MATERIALIZED VIEW ActedInMaterialized AS " +
		"SELECT actor, title FROM Title_Actor;"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

// run with go build createPreviousSources.go
func main() {

	start := time.Now()

	wg := new(sync.WaitGroup)

	wg.Add(10)

	go createComedyMovieNMView(wg)
	go createNonComedyMovieNMView(wg)
	go createComedyActorNMView(wg)
	go createNonComedyActorNMView(wg)
	go createActedInNMView(wg)

	go createComedyMovieMView(wg)
	go createNonComedyMovieMView(wg)
	go createComedyActorMView(wg)
	go createNonComedyActorMView(wg)
	go createActedInMView(wg)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
