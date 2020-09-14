// Daniel Moore
// 9/11/2020
// This code fails on the second insertion and is intended to show the
// ability of a transaction to rollback
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {

	start := time.Now()

	//Open up connection to database
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	//Begin transaction
	tx, err := conn.Begin(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	//1st insert. Is valid
	queryString := "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
		"runtimeMinutes,genres) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err = tx.Exec(context.Background(), queryString, 7153814, "series", "title", "original title",
		false, 1999, 2000, 50, "{'crime', 'fiction'}")
	if err != nil {
		log.Fatal(err)

		//Rollback if error
		err = tx.Rollback(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}

	//2nd insert. Fails and causes rollback
	queryString = "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
		"runtimeMinutes,genres) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err = tx.Exec(context.Background(), queryString, 7153814, "series", "title", "original title",
		false, 1999, 2000, 50, "{'crime', 'fiction'}")
	if err != nil {
		log.Fatal(err)

		//Rollback if error
		err = tx.Rollback(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}

	//3rd insert. Should never even be reached but is valid
	queryString = "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
		"runtimeMinutes,genres) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err = tx.Exec(context.Background(), queryString, 7153815, "series", "title", "original title",
		false, 1999, 2000, 50, "{'crime', 'fiction'}")

	if err != nil {
		log.Fatal(err)

		//Rollback if error
		err = tx.Rollback(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}

	//Commit transaction
	err = tx.Commit(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
