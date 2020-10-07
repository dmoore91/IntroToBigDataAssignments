package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"time"
)

type movieTitleActor struct {
	TitleID   int
	TitleType sql.NullString
	StartYear sql.NullInt32
	Runtime   int
	AvgRating decimal.NullDecimal
	GenreId   int
	Genre     sql.NullString
	MemberId  int
	BirthYear sql.NullInt32
	Role      string
}

func readInData() []movieTitleActor {

	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignment_three")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "SELECT movieID , type, startYear, runtimeMinutes, avgRating, genre_id, genre, member_id, " +
		"birthYear, role " +
		"FROM Movie_Actor_Role"

	rows, err := conn.Query(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}

	var data []movieTitleActor

	defer rows.Close()

	defer rows.Close()

	for rows.Next() {
		var m = movieTitleActor{}
		err = rows.Scan(&m.TitleID, &m.TitleType, &m.StartYear, &m.Runtime, &m.AvgRating, &m.GenreId, &m.Genre,
			&m.MemberId, &m.BirthYear, &m.Role)

		if err != nil {
			log.Error(err)
		}

		data = append(data, m)
	}

	err = conn.Close(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	return data
}

func main() {
	start := time.Now()

	readInData()

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Println(elapsed)
}
