package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func productiveActorsNM(wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT name " +
		"FROM ((SELECT id, name, birthYear, deathYear FROM ComedyActor) UNION (SELECT id, name, birthYear, deathYear FROM NonComedyActor)) AS actor " +
		"WHERE (SELECT COUNT(All_Movie.id) " +
		"FROM ((SELECT id, title, startYear, 'Comedy' AS genre FROM ComedyMovie) UNION (SELECT id, title, startYear, 'Not Comedy' AS genre FROM NonComedyMovie)) AS All_Movie" +
		"INNER JOIN (SELECT actor, title FROM ActedIn) as All_Movie_Actor ON All_Movie_Actor.title = All_Movie.id " +
		"WHERE startYear BETWEEN 2000 AND 2005 and All_Movie_Actor.actor = actor.id) > 10;"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("Non-Materialized Productive Actors " + elapsed.String())

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func productiveActorsM(wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT name " +
		"FROM ((SELECT id, name, birthYear, deathYear FROM ComedyActorMaterialized) UNION (SELECT id, name, birthYear, deathYear FROM NonComedyActorMaterialized)) AS actor " +
		"WHERE (SELECT COUNT(All_Movie.id) " +
		"FROM ((SELECT id, title, startYear, 'Comedy' AS genre FROM ComedyMovieMaterialized) UNION (SELECT id, title, startYear, 'Not Comedy' AS genre FROM NonComedyMovieMaterialized)) AS All_Movie" +
		"INNER JOIN (SELECT actor, title FROM ActedInMaterialized) as All_Movie_Actor ON All_Movie_Actor.title = All_Movie.id " +
		"WHERE startYear BETWEEN 2000 AND 2005 and All_Movie_Actor.actor = actor.id) > 10;"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("Materialized Productive Actors " + elapsed.String())

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	wg := new(sync.WaitGroup)

	wg.Add(2)

	go productiveActorsNM(wg)
	go productiveActorsM(wg)

	wg.Wait()
}
