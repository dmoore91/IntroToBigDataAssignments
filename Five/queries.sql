SELECT actors.name
FROM (SELECT name, COUNT(actor.id)
FROM All_Actor AS actor
INNER JOIN All_Movie_Actor ON All_Movie_Actor.actor = actor.id
INNER JOIN All_Movie_Actor ON All_Movie_Actor.title = All_Movie.id
WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005
GROUP BY name
HAVING COUNT(actor.id) > 10) as actors;

(SELECT name FROM All_Actor AS actor WHERE name LIKE 'Ja%')
EXCEPT
(SELECT name FROM All_Actor AS actor
    INNER JOIN All_Movie_Actor ON All_Movie_Actor.actor = All_Actor.id
    INNER JOIN All_Movie ON All_Movie.id = All_Movie_Actor.title
    WHERE name LIKE 'Ja%' AND All_Movie.genre LIKE 'Comedy')