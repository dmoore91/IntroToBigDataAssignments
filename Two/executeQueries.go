package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"strconv"
	"time"
)

func executeInvalidActorsQuery() {

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT (SELECT COUNT(*) FROM Title_Actor) - (SELECT COUNT(*) FROM Actor_Title_Role) as total_count;"

	var numActors int

	err = conn.QueryRow(context.Background(), queryString).Scan(&numActors)

	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("There are " + strconv.Itoa(numActors) + " invalid actors")
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func actorsNamedPhiAndDidntActIn2014() {

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "(SELECT name FROM Member " +
		"INNER JOIN Title_Actor ON actor = Member.id " +
		"WHERE deathYear IS NULL AND name LIKE 'Phi%' ) " +
		"EXCEPT " +
		"(SELECT name FROM Member " +
		"INNER JOIN Title_Actor ON actor = Member.id INNER JOIN Title ON Title_Actor.title = Title.id " +
		"WHERE deathYear IS NULL AND name LIKE 'Phi%' AND Title.startYear=2014)"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	//End the timing here since printing has nothing to do
	//with the speed of the query
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	defer rows.Close()

	fmt.Println("Actor:")
	for rows.Next() {

		var name string

		err = rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(name)

	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func livingActorsWhoHavePlayedJesusChrist() {

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT name " +
		"FROM Member " +
		"INNER JOIN Actor_Title_Role ON Actor_Title_Role.actor = member.id " +
		"WHERE deathYear IS NOT NULL " +
		"AND " +
		"(role = (SELECT id FROM Role WHERE role LIKE '[Jesus]') OR role = (SELECT id FROM Role WHERE role LIKE '[Christ]'))"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	//End the timing here since printing has nothing to do
	//with the speed of the query
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	defer rows.Close()

	fmt.Println("Actors:")
	for rows.Next() {

		var name string

		err = rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(name)

	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func getProducersGill() {

	start := time.Now()

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT name " +
		"FROM Member " +
		"INNER JOIN Title_Producer ON Title_Producer.producer = Member.id " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title_Producer.title " +
		"INNER JOIN Genre ON Title_Genre.genre = Genre.id " +
		"INNER JOIN Title ON Title_Genre.title = Title.id " +
		"WHERE name LIKE '%Gill%' " +
		"AND Genre.genre LIKE 'Talk-Show' " +
		"AND  startYear = 2017 " +
		"GROUP BY name " +
		"HAVING COUNT (name)=(" +
		"SELECT MAX(tmp.c) " +
		"FROM ( " +
		"SELECT COUNT(name) c " +
		"FROM Member " +
		"INNER JOIN Title_Producer ON Title_Producer.producer = Member.id " +
		"INNER JOIN Title_Genre ON Title_Genre.title = Title_Producer.title " +
		"INNER JOIN Genre ON Title_Genre.genre = Genre.id " +
		"INNER JOIN Title ON Title_Genre.title = Title.id " +
		"WHERE name LIKE '%Gill%' " +
		"AND Genre.genre LIKE 'Talk-Show'" +
		"AND  startYear = 2017" +
		"GROUP BY name) tmp);"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	//End the timing here since printing has nothing to do
	//with the speed of the query
	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println("It took  " + elapsed.String() + " to run this query")

	defer rows.Close()

	fmt.Println("Producers:")
	for rows.Next() {

		var name string

		err = rows.Scan(&name)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(name)

	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func main() {
	//executeInvalidActorsQuery()
	//actorsNamedPhiAndDidntActIn2014()
	//livingActorsWhoHavePlayedJesusChrist()
	getProducersGill()
}
