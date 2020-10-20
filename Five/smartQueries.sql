(SELECT actors.name
FROM (SELECT name, COUNT(actor.id)
FROM ComedyActor AS actor
INNER JOIN ActedIn ON ActedIn.actor = actor.id
INNER JOIN ComedyMovie ON ComedyMovie.title = ComedyMovie.id
WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005
GROUP BY name
HAVING COUNT(actor.id) > 10) as actors;)
UNION
(SELECT actors.name
FROM (SELECT name, COUNT(actor.id)
FROM NonComedyActor AS actor
INNER JOIN ActedIn ON ActedIn.actor = actor.id
INNER JOIN NonComedyMovie ON NonComedyMovie.title = NonComedyMovie.id
WHERE actor.deathYear IS NULL AND  startYear BETWEEN 2000 AND 2005
GROUP BY name
HAVING COUNT(actor.id) > 10) as actors;)


SELECT name
FROM NonComedyActor
WHERE name LIKE 'Ja%'