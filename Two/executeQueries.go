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

func main() {
	executeInvalidActorsQuery()

}
