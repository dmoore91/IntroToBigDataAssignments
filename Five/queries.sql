SELECT name
FROM All_Actor AS actor
WHERE actor.deathYear IS NULL AND (SELECT COUNT(All_Movie.id)
                FROM All_Movie
                INNER JOIN All_Movie_Actor ON All_Movie_Actor.title = All_Movie.id
                WHERE startYear BETWEEN 2000 AND 2005 AND All_Movie_Actor.actor = actor.id) > 10;

SELECT name
FROM All_Actor AS actor
WHERE name LIKE 'Ja%' AND id NOT IN (SELECT id
                                     FROM All_Actor
                                     INNER JOIN All_Movie_Actor ON All_Movie_Actor.actor = All_Actor.id
                                     INNER JOIN All_Movie ON All_Movie.id = All_Movie_Actor.title
                                     WHERE All_Movie.genre LIKE 'Comedy')