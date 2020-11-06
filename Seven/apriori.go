package main

import (
	"context"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
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
			"(SELECT L1.actor1 as actor1, tmp.actor1 as actor2, COUNT(*) " +
			"FROM L1 CROSS JOIN L1 as tmp " +
			"INNER JOIN (SELECT a.actor as actorA, b.actor as actorB FROM Popular_Movie_Actors as a, Popular_Movie_Actors as b WHERE a.title = b.title AND a.actor != b.actor) as a ON " +
			"(a.actorA = L1.actor1 AND a.actorB = tmp.actor1) " +
			"WHERE L1.actor1 < tmp.actor1 " +
			"GROUP BY L1.actor1, tmp.actor1 " +
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

func getBulkInsertSQL(SQLString string, rowValueSQL string, numRows int) string {
	// Combine the base SQL string and N value strings
	valueStrings := make([]string, 0, numRows)
	for i := 0; i < numRows; i++ {
		valueStrings = append(valueStrings, "("+rowValueSQL+")")
	}
	allValuesString := strings.Join(valueStrings, ",")
	SQLString = fmt.Sprintf(SQLString, allValuesString)

	// Convert all of the "?" to "$1", "$2", "$3", etc.
	// (which is the way that pgx expects query variables to be)
	numArgs := strings.Count(SQLString, "?")
	SQLString = strings.ReplaceAll(SQLString, "?", "$%v")
	numbers := make([]interface{}, 0, numRows)
	for i := 1; i <= numArgs; i++ {
		numbers = append(numbers, strconv.Itoa(i))
	}
	return fmt.Sprintf(SQLString, numbers...)
}

func getBulkInsertSQLSimple(SQLString string, numArgsPerRow int, numRows int) string {
	questionMarks := make([]string, 0, numArgsPerRow)
	for i := 0; i < numArgsPerRow; i++ {
		questionMarks = append(questionMarks, "?")
	}
	rowValueSQL := strings.Join(questionMarks, ", ")
	return getBulkInsertSQL(SQLString, rowValueSQL, numRows)
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

	actorToTitleSetMap := getActorToTitleSetMap(conn)

	var frequentActors [][4]int

	for _, set := range potentiallyFrequentSets {

		var actors []int

		it := set.Iterator()

		for c := range it.C {
			actors = append(actors, c.(int))
		}

		titles := actorToTitleSetMap[actors[0]]
		titles = titles.Intersect(actorToTitleSetMap[actors[1]])
		titles = titles.Intersect(actorToTitleSetMap[actors[2]])

		if titles.Cardinality() >= 5 {
			frequentActors = append(frequentActors, [4]int{actors[0], actors[1], actors[2], titles.Cardinality()})
		}
	}

	sqlString := "INSERT INTO L3 (actor1, actor2, actor3, count) VALUES %s"

	numArgsPerRow := 4
	valueArgs := make([]interface{}, 0, numArgsPerRow*len(frequentActors))
	for _, elem := range frequentActors {
		valueArgs = append(valueArgs, elem[0], elem[1], elem[2], elem[3])
	}

	sqlString = getBulkInsertSQLSimple(sqlString, numArgsPerRow, len(frequentActors))

	queryString = "CREATE TABLE L3( " +
		" actor1 INTEGER, " +
		" actor2 INTEGER, " +
		" actor3 INTEGER, " +
		" count INTEGER)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	commandTag, err := conn.Exec(context.Background(), sqlString, valueArgs...)

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

func getQueryStringForPreviousLattice(lattice int) string {

	queryString := "SELECT "

	for i := 1; i <= lattice; i++ {
		queryString += "actor"
		queryString += strconv.Itoa(i)
		queryString += " "
	}

	queryString += "FROM L"
	queryString += strconv.Itoa(lattice)

	return queryString
}

func genericLatticeGeneration() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	i := 2

	// Don't actually want infinite loop, we just don't know when it'll finish
	for {

		queryString := getQueryStringForPreviousLattice(i - 1)

		fmt.Println(queryString)

		rows, err := conn.Query(context.Background(), queryString)

		if err != nil {
			log.Error(err)
		}

		//var l2SetList []mapset.Set

		// Each row becomes a set
		// Union with other row
		// If union is size 3 (... or whatever size we want), then keep

		for rows.Next() {
			var actors []int
			err = rows.Scan(&actors)

			if err != nil {
				log.Error(err)
			}

			//tmpSet := mapset.NewSet()
			//tmpSet.Add(actor1)
			//tmpSet.Add(actor2)
			//
			//l2SetList = append(l2SetList, tmpSet)
		}

		rows.Close()
		break

	}
}

// Minimum support is 5
// Therefore we must only keep entries with a count >=5 for
// all tables
func main() {
	start := time.Now()

	//createL1()
	//createL2()
	//createL3()

	genericLatticeGeneration()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
