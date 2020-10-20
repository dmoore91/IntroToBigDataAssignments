SELECT name
FROM All_Actor AS actor
WHERE (SELECT COUNT(All_Movie.id)
                FROM All_Movie
                INNER JOIN All_Movie_Actor ON All_Movie_Actor.title = All_Movie.id
                WHERE startYear BETWEEN 2000 AND 2005 and All_Movie_Actor.actor = actor.id) > 10;