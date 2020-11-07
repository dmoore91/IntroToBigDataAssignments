package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"strconv"
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

func generateSelectString(lattice int) string {

	selectString := "SELECT "

	for i := 1; i < lattice; i++ {
		selectString += "p.actor"
		selectString += strconv.Itoa(i)
		selectString += " as actor"
		selectString += strconv.Itoa(i)
		selectString += ", "
	}

	selectString += "q.actor"
	selectString += strconv.Itoa(lattice - 1)
	selectString += " as actor"
	selectString += strconv.Itoa(lattice)
	selectString += ", COUNT(*) as count "

	return selectString
}

func generateFromStatement(lattice int) string {

	alphabetMap := make(map[int]string)
	alphabetMap[0] = "a"
	alphabetMap[1] = "b"
	alphabetMap[2] = "c"
	alphabetMap[3] = "d"
	alphabetMap[4] = "e"
	alphabetMap[5] = "f"
	alphabetMap[6] = "g"
	alphabetMap[7] = "h"
	alphabetMap[8] = "i"
	alphabetMap[9] = "j"
	alphabetMap[10] = "k"
	alphabetMap[11] = "l"
	alphabetMap[12] = "m"
	alphabetMap[13] = "n"
	alphabetMap[14] = "o"
	alphabetMap[15] = "r"
	alphabetMap[16] = "s"
	alphabetMap[17] = "t"
	alphabetMap[18] = "u"
	alphabetMap[19] = "v"
	alphabetMap[20] = "w"
	alphabetMap[21] = "x"
	alphabetMap[22] = "y"
	alphabetMap[23] = "z"

	fromString := "FROM "
	fromString += "L"
	fromString += strconv.Itoa(lattice - 1)
	fromString += " p, "
	fromString += "L"
	fromString += strconv.Itoa(lattice - 1)
	fromString += " q"

	for i := 0; i < lattice; i++ {
		fromString += ", "
		fromString += "Popular_Movie_Actors "
		fromString += alphabetMap[i]
	}
	fromString += " "

	return fromString
}

func generateWhereString(lattice int) string {

	whereString := "WHERE "

	for i := 1; i <= lattice-2; i++ {
		whereString += "p.actor"
		whereString += strconv.Itoa(i)
		whereString += " = "
		whereString += "q.actor"
		whereString += strconv.Itoa(i)
		whereString += " AND "
	}

	whereString += "p.actor"
	whereString += strconv.Itoa(lattice - 1)
	whereString += " < "
	whereString += "q.actor"
	whereString += strconv.Itoa(lattice - 1)

	alphabetMap := make(map[int]string)
	alphabetMap[0] = "a"
	alphabetMap[1] = "b"
	alphabetMap[2] = "c"
	alphabetMap[3] = "d"
	alphabetMap[4] = "e"
	alphabetMap[5] = "f"
	alphabetMap[6] = "g"
	alphabetMap[7] = "h"
	alphabetMap[8] = "i"
	alphabetMap[9] = "j"
	alphabetMap[10] = "k"
	alphabetMap[11] = "l"
	alphabetMap[12] = "m"
	alphabetMap[13] = "n"
	alphabetMap[14] = "o"
	alphabetMap[15] = "r"
	alphabetMap[16] = "s"
	alphabetMap[17] = "t"
	alphabetMap[18] = "u"
	alphabetMap[19] = "v"
	alphabetMap[20] = "w"
	alphabetMap[21] = "x"
	alphabetMap[22] = "y"
	alphabetMap[23] = "z"

	for i := 1; i < lattice; i++ {
		whereString += " AND "
		whereString += alphabetMap[i-1]
		whereString += ".actor = p.actor"
		whereString += strconv.Itoa(i)
	}

	whereString += " AND "
	whereString += alphabetMap[lattice-1]
	whereString += ".actor = q.actor"
	whereString += strconv.Itoa(lattice - 1)

	for i := 0; i < lattice-1; i++ {
		whereString += " AND "
		whereString += alphabetMap[i]
		whereString += ".title = "
		whereString += alphabetMap[i+1]
		whereString += ".title "
	}

	return whereString
}

func generateGroupBy(lattice int) string {

	groupByString := "GROUP BY "

	for i := 1; i < lattice; i++ {
		groupByString += "p.actor"
		groupByString += strconv.Itoa(i)
		groupByString += ", "
	}

	groupByString += "q.actor"
	groupByString += strconv.Itoa(lattice - 1)
	groupByString += " "

	return groupByString
}

func generateAprioriLattices() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	lattice := 2

	for {

		queryString := "CREATE TABLE L" + strconv.Itoa(lattice) + " AS ("
		queryString += generateSelectString(lattice)
		queryString += generateFromStatement(lattice)
		queryString += generateWhereString(lattice)
		queryString += generateGroupBy(lattice)
		queryString += "HAVING COUNT(*) >= 5)"

		fmt.Println(queryString)

		commandTag, err := conn.Exec(context.Background(), queryString)

		if err != nil {
			log.Error(err)
		}

		if commandTag.RowsAffected() == 0 {
			break
		} else {
			fmt.Println("Lattice: " + strconv.Itoa(lattice) + " has " + strconv.Itoa(int(commandTag.RowsAffected())) + " entries")
		}

		lattice += 1
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Error(err)
	}

}

func getMembers() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT a.name, b.name, c.name, d.name, e.name, f.name, g.name " +
		"FROM L7 " +
		"INNER JOIN Member a ON a.id = actor1 " +
		"INNER JOIN Member b ON b.id = actor2 " +
		"INNER JOIN Member c ON c.id = actor3 " +
		"INNER JOIN Member d ON d.id = actor4 " +
		"INNER JOIN Member e ON e.id = actor5 " +
		"INNER JOIN Member f ON f.id = actor6 " +
		"INNER JOIN Member g ON g.id = actor7"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Error(err)
	}

	defer rows.Close()

	for rows.Next() {

		var actor1 string
		var actor2 string
		var actor3 string
		var actor4 string
		var actor5 string
		var actor6 string
		var actor7 string

		err = rows.Scan(&actor1, &actor2, &actor3, &actor4, &actor5, &actor6, &actor7)
		if err != nil {
			log.Error(err)
		}

		fmt.Println(actor1 + "," + actor2 + "," + actor3 + "," + actor4 + "," + actor5 + "," + actor6 + "," + actor7)

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
	//createL3()

	//generateAprioriLattices()
	getMembers()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
