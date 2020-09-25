/*2.1*/
/*
3.1

First we aggregate, then we gather up the results of the aggregation. Next, we aggregate again and perform a hash join,
which is when you join on the hash of the data. The path splits from here, and we perform a sequential scan on one branch
and on the other branch we hash, then sequentially scan
*/
SELECT COUNT(Title_Actor.title) FROM Title_Actor LEFT OUTER JOIN Actor_Title_Role ON Actor_Title_Role.title = Title_Actor.title;

/*2.2*/
(SELECT name FROM Member INNER JOIN Title_Actor ON actor = Member.id WHERE deathYear IS NULL 	AND name LIKE 'Phi%' ) EXCEPT (SELECT name FROM Member INNER JOIN Title_Actor ON actor = Member.id INNER JOIN Title ON Title_Actor.title = Title.id WHERE deathYear IS NULL AND name LIKE 'Phi%'  AND Title.startYear=2014);
	
/*2.3*/
SELECT name, COUNT(name) FROM Member INNER JOIN Title_Producer ON Title_Producer.producer = Member.id INNER JOIN Title_Genre ON Title_Genre.title = Title_Producer.title INNER JOIN Genre ON Title_Genre.genre = Genre.id INNER JOIN Title ON Title_Genre.title = Title.id WHERE name LIKE '%Gill%'  AND Genre.genre LIKE 'Talk-Show' AND  startYear = 2017 GROUP BY name HAVING COUNT (name)=(  SELECT MAX(tmp.c) FROM (SELECT COUNT(name) c FROM Member INNER JOIN Title_Producer ON Title_Producer.producer = Member.id INNER JOIN Title_Genre ON Title_Genre.title = Title_Producer.title INNER JOIN Genre ON Title_Genre.genre = Genre.id INNER JOIN Title ON Title_Genre.title = Title.id WHERE name LIKE '%Gill%'  AND Genre.genre LIKE 'Talk-Show' AND  startYear = 2017 GROUP BY name) tmp);

/*2.4*/
SELECT name FROM Member INNER JOIN Title_Producer ON Title_Producer.producer = Member.id INNER JOIN Title ON Title.id = Title_Producer.title WHERE deathYear IS NOT NULL AND runtimeMinutes > 120 GROUP BY name HAVING COUNT (name)=(SELECT MAX(tmp.c) FROM (SELECT COUNT(name) c FROM Member INNER JOIN Title_Producer ON Title_Producer.producer = Member.id INNER JOIN Title ON Title.id = Title_Producer.title WHERE deathYear IS NOT NULL AND runtimeMinutes > 120 GROUP BY name) tmp);
	
/*2.5*/
SELECT name FROM Member INNER JOIN Actor_Title_Role ON Actor_Title_Role.actor = member.id WHERE deathYear IS NOT NULL AND (role = (SELECT id FROM Role WHERE role LIKE '[Jesus]') OR  role = (SELECT id FROM Role WHERE role LIKE '[Christ]'));