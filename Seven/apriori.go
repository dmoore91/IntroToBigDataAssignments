package main

import (
	"context"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"math"
	"math/bits"
	"strconv"
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

	uniqueActors := mapset.NewSet()

	defer rows.Close()

	for rows.Next() {
		var actor1 int
		var actor2 int
		err = rows.Scan(&actor1, &actor2)

		if err != nil {
			log.Error(err)
		}

		uniqueActors.Add(actor1)
		uniqueActors.Add(actor2)
	}

	var actors []string

	it := uniqueActors.Iterator()

	for actor := range it.C {
		i := actor.(int)
		actors = append(actors, strconv.Itoa(i))
	}

	combos := combinations(actors, 3)

	fmt.Println(combos)

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}
}

func combinations(set []string, n int) (subsets [][]string) {

	if n > len(set) {
		n = len(set)
	}

	numCombs := int(math.Pow(float64(len(set)), 2.0))
	length := len(set)

	fmt.Println(length)

	// Go through all possible combinations of objects
	// from 1 (only first object in subset) to 2^length (all objects in subset)
	for subsetBits := 1; subsetBits < numCombs; subsetBits += 1 {
		if n > 0 && bits.OnesCount(uint(subsetBits)) != n {
			continue
		}

		var subset []string

		for object := 0; object < length; object++ {
			// checks if object is contained in subset
			// by checking if bit 'object' is set in subsetBits
			if (subsetBits>>object)&1 == 1 {
				// add object to subset
				subset = append(subset, set[object])
			}
		}
		// add subset to subsets
		subsets = append(subsets, subset)
	}
	return subsets
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
