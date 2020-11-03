package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"time"
)

func createPopularMovieActorsTable() {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_seven")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE TABLE Popular_Movie_Actors AS " +
		"(SELECT actor, Title_Actor.title " +
		"FROM Title_Actor INNER JOIN Title ON Title_Actor.title = Title.id " +
		"WHERE avgRating>5 AND type LIKE 'movie')"

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

func main() {
	start := time.Now()

	createPopularMovieActorsTable()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
