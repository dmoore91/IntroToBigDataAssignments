BEGIN;

CREATE TABLE IF NOT EXISTS title(
    titleID INTEGER PRIMARY KEY ,
    titleType text NOT NULL ,
    primaryTitle text NOT NULL ,
    originalTitle text NOT NULL ,
    isAdult BOOLEAN NOT NULL ,
    startYear INTEGER NOT NULL ,
    endYear INTEGER NOT NULL ,
    runtimeMinutes INTEGER NOT NULL ,
    genres text array
);

CREATE TABLE IF NOT EXISTS people(
     peopleID INTEGER PRIMARY KEY ,
     primaryName VARCHAR(150) NOT NULL ,
     birthYear INTEGER NOT NULL ,
     deathYear INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS crew(
   crewID INTEGER NOT NULL PRIMARY KEY ,
   titleID INTEGER NOT NULL ,
   FOREIGN KEY(titleID)
       REFERENCES title(titleID)
);

CREATE TABLE IF NOT EXISTS episode(
    titleID INTEGER NOT NULL ,
    seriesTitleID INTEGER NOT NULL ,
    seasonNumber INTEGER NOT NULL ,
    episodeNumber INTEGER NOT NULL ,
    FOREIGN KEY(titleID)
        REFERENCES title(titleID),
    FOREIGN KEY(seriesTitleID)
        REFERENCES crew(crewID)
);

CREATE TABLE IF NOT EXISTS directors(
    crewID INTEGER NOT NULL ,
    peopleID INTEGER NOT NULL ,
    FOREIGN KEY(crewID)
        REFERENCES crew(crewID) ,
    FOREIGN KEY(peopleID)
        REFERENCES people(peopleID)
);

CREATE TABLE IF NOT EXISTS writers(
    crewID INTEGER NOT NULL ,
    peopleID INTEGER NOT NULL ,
    FOREIGN KEY(crewID)
        REFERENCES crew(crewID) ,
    FOREIGN KEY(peopleID)
        REFERENCES people(peopleID)
);

CREATE TABLE IF NOT EXISTS principal(
     titleID INTEGER NOT NULL ,
     ordering INTEGER NOT NULL ,
     peopleID INTEGER NOT NULL ,
     category text NOT NULL ,
     FOREIGN KEY(titleID)
         REFERENCES title(titleID) ,
     FOREIGN KEY(peopleID)
         REFERENCES people(peopleID)
);

CREATE TABLE IF NOT EXISTS ratings(
    titleID INTEGER NOT NULL ,
    averageRating DECIMAL NOT NULL ,
    numVotes INTEGER NOT NULL ,
    FOREIGN KEY(titleID)
        REFERENCES title(titleID)
);

COMMIT;