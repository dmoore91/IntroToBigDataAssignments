package main

import (
	"context"
	"fmt"
	mapset "github.com/deckarep/golang-set"
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

func getActorToTitleSetMap(conn *pgx.Conn) map[int]mapset.Set {

	queryString := "SELECT actor, title FROM Popular_Movie_Actors"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	defer rows.Close()

	actorToTitleSet := make(map[int]mapset.Set)

	defer rows.Close()

	for rows.Next() {
		var actor int
		var title int
		err = rows.Scan(&actor, &title)

		if err != nil {
			log.Error(err)
		}

		_, ok := actorToTitleSet[actor]

		if ok {
			actorToTitleSet[actor].Add(title)
		} else {
			set := mapset.NewSet()
			set.Add(title)
			actorToTitleSet[actor] = set
		}
	}

	return actorToTitleSet
}

func createL3() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	//Below section gets us all the unique actors in L2

	queryString := "SELECT actor1, actor2 FROM L2"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	defer rows.Close()

	var l2SetList []mapset.Set

	defer rows.Close()

	// Each row becomes a set
	// Union with other row
	// If union is size 3 (... or whatever size we want), then keep

	for rows.Next() {
		var actor1 int
		var actor2 int
		err = rows.Scan(&actor1, &actor2)

		if err != nil {
			log.Error(err)
		}

		tmpSet := mapset.NewSet()
		tmpSet.Add(actor1)
		tmpSet.Add(actor2)

		l2SetList = append(l2SetList, tmpSet)
	}

	var potentiallyFrequentSets []mapset.Set

	for i, a := range l2SetList {
		for j, b := range l2SetList {

			if i != j {
				u := a.Union(b)
				if u.Cardinality() == 3 {
					potentiallyFrequentSets = append(potentiallyFrequentSets, u)
				}
			}
		}
	}

	fmt.Println(len(potentiallyFrequentSets))

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
