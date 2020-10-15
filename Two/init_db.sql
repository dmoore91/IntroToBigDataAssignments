BEGIN;

CREATE TABLE IF NOT EXISTS Title(
    id INTEGER PRIMARY KEY ,
    type text ,
    title text ,
    originalTitle text ,
    startYear INTEGER  ,
    endYear INTEGER ,
    runtimeMinutes INTEGER ,
    avgRating DECIMAL ,
    numVotes INTEGER
);

CREATE TABLE IF NOT EXISTS Genre(
    id INTEGER PRIMARY KEY ,
    genre VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Title_Genre(
    title INTEGER NOT NULL ,
    genre INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Member(
    id INTEGER PRIMARY KEY ,
    name text NOT NULL ,
    birthYear INTEGER ,
    deathYear INTEGER
);

CREATE TABLE IF NOT EXISTS Title_Writer(
    writer INTEGER NOT NULL ,
    title INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Title_Director(
    director INTEGER NOT NULL ,
    title INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Title_Actor(
    actor INTEGER NOT NULL ,
    title INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Title_Producer(
    producer INTEGER NOT NULL ,
    title INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Role(
    id SERIAL PRIMARY KEY ,
    role text NOT NULL
);

CREATE TABLE IF NOT EXISTS Actor_Title_Role(
    actor INTEGER NOT NULL ,
    title INTEGER NOT NULL ,
    role INTEGER NOT NULL
);

COMMIT;