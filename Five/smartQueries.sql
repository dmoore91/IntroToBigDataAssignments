SELECT name
FROM ((SELECT id, name, birthYear, deathYear FROM ComedyActor) UNION (SELECT id, name, birthYear, deathYear FROM NonComedyActor)) AS actor
WHERE (SELECT COUNT(All_Movie.id)
FROM ((SELECT id, title, startYear, 'Comedy' AS genre FROM ComedyMovie) UNION (SELECT id, title, startYear, 'Not Comedy' AS genre FROM NonComedyMovie)) AS All_Movie
INNER JOIN (SELECT actor, title FROM ActedIn) as All_Movie_Actor ON All_Movie_Actor.title = All_Movie.id
WHERE startYear BETWEEN 2000 AND 2005 and All_Movie_Actor.actor = actor.id) > 10;

SELECT name
FROM NonComedyActor
WHERE name LIKE 'Ja%'