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
		"(SELECT actor, COUNT(actor) " +
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
			"(SELECT L1.actor as actor1, tmp.actor as actor2, COUNT(*) " +
			"FROM L1 CROSS JOIN L1 as tmp " +
			"INNER JOIN (SELECT a.actor as actorA, b.actor as actorB FROM Popular_Movie_Actors as a, Popular_Movie_Actors as b WHERE a.title = b.title AND a.actor != b.actor) as a ON " +
			"(a.actorA = L1.actor AND a.actorB = tmp.actor) " +
			"WHERE L1.actor < tmp.actor " +
			"GROUP BY L1.actor, tmp.actor " +
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
	createL2()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}