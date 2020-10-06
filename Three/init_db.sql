BEGIN ;

CREATE TABLE IF NOT EXISTS Movie_Actor_Role(
    movieID INTEGER NOT NULL ,
    type text ,
    startYear INTEGER ,
    runtimeMinutes INTEGER ,
    avgRating DECIMAL ,
    genre_id INTEGER ,
    genre VARCHAR(100) ,
    member_id INTEGER ,
    birthYear INTEGER ,
    role text
);

COMMIT ;