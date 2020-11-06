package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"time"
)

func createL1() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE TABLE L1 AS " +
		"(SELECT actor as actor1, COUNT(actor) " +
		"FROM Popular_Movie_Actors " +
		"GROUP BY actor	" +
		"HAVING COUNT(actor) >= 5)"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func createL2() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	queryString :=
		"CREATE TABLE L2 AS " +
			"(SELECT p.actor1 as actor1, q.actor1 as actor2, COUNT(*) as count " +
			"FROM L1 p, L1 q, Popular_Movie_Actors a, Popular_Movie_Actors b " +
			"WHERE p.actor1 < q.actor1 AND a.actor = p.actor1 AND b.actor = q.actor1 AND a.title = b.title " +
			"GROUP BY p.actor1, q.actor1 " +
			"HAVING COUNT(*) >= 5)"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func createL3() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	queryString :=
		"CREATE TABLE L3 AS " +
			"(SELECT p.actor1 as actor1, p.actor2 as actor2, q.actor2 as actor3, COUNT(*) as count " +
			"FROM L2 p, L2 q, Popular_Movie_Actors a, Popular_Movie_Actors b, Popular_Movie_Actors c " +
			"WHERE p.actor1 = q.actor1 AND p.actor2 < q.actor2 AND a.actor = p.actor1 AND b.actor = p.actor2 " +
			"AND c.actor = q.actor2 AND a.title = b.title AND b.title = c.title " +
			"GROUP BY p.actor1, p.actor2, q.actor2 " +
			"HAVING COUNT(*) >= 5)"

	commandTag, err := conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	if commandTag.RowsAffected() == 0 {
		log.Error(err)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

// Minimum support is 5
// Therefore we must only keep entries with a count >=5 for
// all tables
func main() {
	start := time.Now()

	//createL1()
	//createL2()
	createL3()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
