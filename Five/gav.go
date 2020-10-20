package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func allMovieNM(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW All_Movie AS " +
		"(SELECT id, title, startYear, 'Comedy' AS genre FROM ComedyMovie) " +
		"UNION " +
		"(SELECT id, title, startYear, 'Not Comedy' AS genre FROM NonComedyMovie);"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func allActorNM(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW All_Actor AS " +
		"(SELECT id, name, birthYear, deathYear FROM ComedyActor) " +
		"UNION " +
		"(SELECT id, name, birthYear, deathYear FROM NonComedyActor)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func allMovieActorNM(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW All_Movie_Actor AS " +
		"SELECT actor, title FROM ActedIn"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func allMovieM(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW All_Movie_Materialized AS " +
		"(SELECT id, title, startYear, 'Comedy' AS genre FROM ComedyMovieMaterialized) " +
		"UNION " +
		"(SELECT id, title, startYear, 'Not Comedy' AS genre FROM NonComedyMovieMaterialized);"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func allActorM(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW All_Actor_Materialized AS " +
		"(SELECT id, name, birthYear, deathYear FROM ComedyActorMaterialized) " +
		"UNION " +
		"(SELECT id, name, birthYear, deathYear FROM NonComedyActorMaterialized)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func allMovieActorM(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE VIEW All_Movie_Actor_Materialized AS " +
		"SELECT actor, title FROM ActedInMaterialized "

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
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

	wg.Add(6)

	go allMovieNM(wg)
	go allActorNM(wg)
	go allMovieActorNM(wg)

	go allMovieM(wg)
	go allActorM(wg)
	go allMovieActorM(wg)

	wg.Wait()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
