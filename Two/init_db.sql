BEGIN;

CREATE TABLE IF NOT EXISTS Title(
    id INTEGER PRIMARY KEY ,
    type text NOT NULL ,
    originalTitle text NOT NULL ,
    startYear INTEGER NOT NULL ,
    endYear INTEGER NOT NULL ,
    runtimeMinutes INTEGER NOT NULL ,
    avgRating DECIMAL NOT NULL ,
    numVotes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Genre(
    id SERIAL PRIMARY KEY ,
    genre VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Title_Genre(
    title INTEGER NOT NULL ,
    genre INTEGER NOT NULL ,
    FOREIGN KEY(title)
        REFERENCES Title(id) ,
    FOREIGN KEY(genre)
        REFERENCES Genre(id)
);

CREATE TABLE IF NOT EXISTS Member(
    id SERIAL PRIMARY KEY ,
    name text NOT NULL ,
    birthYear INTEGER NOT NULL ,
    deathYear INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Title_Actor(
    actor INTEGER NOT NULL ,
    title INTEGER NOT NULL ,
    FOREIGN KEY(title)
        REFERENCES Title(id) ,
    FOREIGN KEY(actor)
        REFERENCES Member(id)
);

CREATE TABLE IF NOT EXISTS Title_Writer(
    writer INTEGER NOT NULL ,
    title INTEGER NOT NULL ,
    FOREIGN KEY(title)
      REFERENCES Title(id) ,
    FOREIGN KEY(writer)
      REFERENCES Member(id)
);

CREATE TABLE IF NOT EXISTS Title_Director(
    director INTEGER NOT NULL ,
    title INTEGER NOT NULL ,
    FOREIGN KEY(title)
       REFERENCES Title(id) ,
    FOREIGN KEY(director)
       REFERENCES Member(id)
);

CREATE TABLE IF NOT EXISTS Title_Producer(
    producer INTEGER NOT NULL ,
    title INTEGER NOT NULL ,
    FOREIGN KEY(title)
        REFERENCES Title(id) ,
    FOREIGN KEY(producer)
        REFERENCES Member(id)
);

CREATE TABLE IF NOT EXISTS Role(
    id SERIAL PRIMARY KEY ,
    role VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS Actor_Title_Role(
    actor INTEGER NOT NULL ,
    title INTEGER NOT NULL ,
    role INTEGER NOT NULL ,
    FOREIGN KEY(role)
        REFERENCES Role(id) ,
    FOREIGN KEY(actor, title)
        REFERENCES Title_Actor(actor, title)
);

COMMIT;