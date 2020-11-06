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

func combinationsOf3(set []int, conn *pgx.Conn) mapset.Set {

	length := len(set)

	coms := mapset.NewSet()

	actorToTitleSet := getActorToTitleSetMap(conn)

	for i := 0; i < length; i++ {
		for j := 0; j < length; j++ {
			for k := 0; k < length; k++ {

				titles := actorToTitleSet[set[i]]
				titles = titles.Intersect(actorToTitleSet[set[j]])
				titles = titles.Intersect(actorToTitleSet[set[k]])

				if titles.Cardinality() >= 5 {

					tmp := mapset.NewSet()
					tmp.Add(set[i])
					tmp.Add(set[j])
					tmp.Add(set[k])

					coms.Add(tmp)
				}
			}
		}
		fmt.Println(coms.Cardinality())
	}

	return coms
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

	var actors []int

	it := uniqueActors.Iterator()

	for actor := range it.C {
		i := actor.(int)
		actors = append(actors, i)
	}

	//Get all unique combinations of 3 actors
	combos := combinationsOf3(actors, conn)

	fmt.Println(len(actors))
	fmt.Println(combos.Cardinality())

	//valueStrings := make([]string, 0,combos.Cardinality())
	//valueArgs := make([]interface{}, 0, combos.Cardinality() * 3)
	//i := 0
	//
	//comboIt := combos.Iterator()
	//
	//for c := range comboIt.C{
	//
	//	tmp := c.(string)
	//
	//	parts := strings.Split(tmp, ",")
	//
	//	var arr [3]int
	//
	//	arr[0], _ = strconv.Atoi(parts[0])
	//	arr[1], _ = strconv.Atoi(parts[1])
	//	arr[2], _ = strconv.Atoi(parts[2])
	//
	//	titles := actorToTitleSet[arr[0]]
	//	titles = titles.Intersect(actorToTitleSet[arr[1]])
	//	titles = titles.Intersect(actorToTitleSet[arr[2]])
	//
	//	if titles.Cardinality() >= 5 {
	//		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d)", i*4+1, i*4+2, i*4+3, i*4+4))
	//		valueArgs = append(valueArgs, arr[0])
	//		valueArgs = append(valueArgs, arr[1])
	//		valueArgs = append(valueArgs, arr[2])
	//		valueArgs = append(valueArgs, titles.Cardinality())
	//		i++
	//	}
	//}

	//fmt.Println("There are " + strconv.Itoa(i) + " frequent itemsets of size 3")
	//
	//queryString = "CREATE TABLE L3"
	//
	//_, err = conn.Exec(context.Background(), queryString)
	//
	//if err != nil {
	//	//Needs to be fatal because we will blow up the next chunk of code with errors if it isn't
	//	log.Fatal(err)
	//}
	//
	//stmt := fmt.Sprintf("INSERT INTO L3 (actor1, actor2, actor3, count) VALUES %s", strings.Join(valueStrings, ","))
	//
	//commandTag, err := conn.Exec(context.Background(), stmt, valueArgs...)

	//if err != nil {
	//	log.Error(err)
	//}
	//
	//if commandTag.RowsAffected() == 0 {
	//	log.Error(err)
	//}

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
