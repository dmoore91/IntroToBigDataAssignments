package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func productiveActorsSmartNM(wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT actors.name " +
		"FROM (SELECT name, COUNT(actor.id) " +
		"FROM ComedyActor AS actor	" +
		"INNER JOIN ActedIn ON ActedIn.actor = actor.id " +
		"INNER JOIN ComedyMovie ON ActedIn.title = ComedyMovie.id " +
		"WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005 " +
		"GROUP BY name " +
		"HAVING COUNT(actor.id) > 10" +
		"UNION " +
		"SELECT name, COUNT(actor.id) " +
		"FROM NonComedyActor AS actor " +
		"INNER JOIN ActedIn ON ActedIn.actor = actor.id " +
		"INNER JOIN NonComedyMovie ON ActedIn.title = NonComedyMovie.id " +
		"WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005 " +
		"GROUP BY name " +
		"HAVING COUNT(actor.id) > 10) as actors"

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

func productiveActorsSmartM(wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT actors.name " +
		"FROM (SELECT name, COUNT(actor.id) " +
		"FROM ComedyActorMaterialized AS actor	" +
		"INNER JOIN ActedInMaterialized ON ActedInMaterialized.actor = actor.id " +
		"INNER JOIN ComedyMovieMaterialized ON ActedInMaterialized.title = ComedyMovieMaterialized.id " +
		"WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005 " +
		"GROUP BY name " +
		"HAVING COUNT(actor.id) > 10" +
		"UNION " +
		"SELECT name, COUNT(actor.id) " +
		"FROM NonComedyActorMaterialized AS actor " +
		"INNER JOIN ActedInMaterialized ON ActedInMaterialized.actor = actor.id " +
		"INNER JOIN NonComedyMovieMaterialized ON ActedInMaterialized.title = NonComedyMovieMaterialized.id " +
		"WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005 " +
		"GROUP BY name " +
		"HAVING COUNT(actor.id) > 10) as actors"

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

func notFunnyActorsSmartNM(wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT name " +
		"FROM NonComedyActor " +
		"WHERE name LIKE 'Ja%'"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("Non-Materialized Not funny Actors " + elapsed.String())

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func notFunnyActorsSmartM(wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_five")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT name " +
		"FROM NonComedyActorMaterialized " +
		"WHERE name LIKE 'Ja%'"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("Materialized Not funny Actors " + elapsed.String())

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	wg := new(sync.WaitGroup)

	wg.Add(4)

	go productiveActorsSmartNM(wg)
	go productiveActorsSmartM(wg)
	go notFunnyActorsSmartNM(wg)
	go notFunnyActorsSmartM(wg)

	wg.Wait()
}
