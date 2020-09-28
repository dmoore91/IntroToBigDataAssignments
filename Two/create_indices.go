package main

import (
	"context"
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
)

func indexIdInTitle() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE INDEX title_id ON title(id)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}
}

func indexOnRoleInActorTitleRole() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE INDEX role_index ON Actor_Title_Role(role)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}
}

func indexOnGenreInTitleGenre() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE INDEX genre_index ON Title_Genre(genre)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}
}

func indexOnActorInTitleActor() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE INDEX actor_index ON Title_Actor(actor)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}
}

func indexOnTitleInActorTitleRole() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres@localhost:5432/assignmenttwo")
	if err != nil {
		log.Fatal(err)
	}

	queryString := "CREATE INDEX actor_title_index ON Actor_Title_Role(title)"

	_, err = conn.Exec(context.Background(), queryString)

	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	//Create b-tree index on title_id in title
	indexIdInTitle()

	//Create b-tree index on role in actor title role
	indexOnRoleInActorTitleRole()

	//Create b-tree index on genre in title_genre
	indexOnGenreInTitleGenre()

	//Create b-tree index on actor in title_actor
	indexOnActorInTitleActor()

	//Create hash index on title in actor_title_role
	indexOnTitleInActorTitleRole()

}
