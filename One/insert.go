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

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmentone")
	if err != nil {
		log.Fatal(err)
	}

	tx, err := conn.Begin(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	queryString := "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
		"runtimeMinutes,genres) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err = tx.Exec(context.Background(), queryString, 7153814, "series", "title", "original title",
		false, 1999, 2000, 50, "{'crime', 'fiction'}")
	if err != nil {
		log.Fatal(err)

		err = tx.Rollback(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}

	queryString = "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
		"runtimeMinutes,genres) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err = tx.Exec(context.Background(), queryString, 7153814, "series", "title", "original title",
		false, 1999, 2000, 50, "{'crime', 'fiction'}")
	if err != nil {
		log.Fatal(err)

		err = tx.Rollback(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}

	queryString = "INSERT INTO title(titleID,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear," +
		"runtimeMinutes,genres) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

	_, err = tx.Exec(context.Background(), queryString, 7153815, "series", "title", "original title",
		false, 1999, 2000, 50, "{'crime', 'fiction'}")

	if err != nil {
		log.Fatal(err)

		err = tx.Rollback(context.Background())
		if err != nil {
			log.Fatal(err)
		}
	}

	err = tx.Commit(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
