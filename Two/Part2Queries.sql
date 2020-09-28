/*2.1*/
/*
3.1

First we aggregate, then we gather up the results of the aggregation. Next, we aggregate again and perform a hash join,
which is when you join on the hash of the data. The path splits from here, and we perform a sequential scan on one branch
and on the other branch we hash the data, then sequentially scan it
*/
SELECT COUNT(Title_Actor.title)
FROM Title_Actor LEFT OUTER JOIN Actor_Title_Role ON Actor_Title_Role.title = Title_Actor.title;

/*2.2*/
/*
3.2

This execution plan starts with a SETOP and append. I think these two are related to the EXCEPT operation. At this point it
branches in 2, which is presumably the two queries we are running. Both branches start off with a subquery scan and gather.
From here they diverge. The first query does a hash join, then sequential scan for both parts of the ON in the join.
The second query uses an index scan to join on title and a hash join to join on actor and Member.id. The join on actor
and member.id is the exact same in both branches.
*/
(SELECT name
    FROM Member
    INNER JOIN Title_Actor ON actor = Member.id
    WHERE deathYear IS NULL AND name LIKE 'Phi%' )
EXCEPT
(SELECT name
    FROM Member
    INNER JOIN Title_Actor ON actor = Member.id
    INNER JOIN Title ON Title_Actor.title = Title.id
    WHERE deathYear IS NULL AND name LIKE 'Phi%'  AND Title.startYear=2014);
	
/*2.3*/
/*
3.3

Both queries start with an aggregation on member.name. The first query starts with 2 aggregations, then a gather merge
then another aggregation. After which we sort by member.name. At this point the plan diverges. One part performs an index
scan while the other performs the inner joins as a nested loop. From here is splits where one side is an index scan and
the other is a hash join on title and the genre title. The hash join branches into a sequential scan on title_producer.
The other branch starts with a hash, which then leads to a hash join. This hash join splits into a sequential scan and
another hash, which ends in an index scan.

The second query starts with a gather merge, then an aggregation then it sorts on member.name. At this point we start
performing inner joins via a nested loop. One branch is an index scan on the primary key of member, the other is
another nested loop. This nested loop then branches into an index scan on the primary key of the title table. The other
branch from here is a hash join which is used to perform the inner join on genre and producer title. From here we
branch again. One side of the branch being a sequential scan on the title_producer table. The other branch being a hash,
then a join on title_genre and genre_id. At this point we once again branch. The one branch is a sequential scan on
the title_genre table. The other branch is a hash then index scan on the genre key of the genre table
*/
SELECT name, COUNT(name)
    FROM Member
    INNER JOIN Title_Producer ON Title_Producer.producer = Member.id
    INNER JOIN Title_Genre ON Title_Genre.title = Title_Producer.title
    INNER JOIN Genre ON Title_Genre.genre = Genre.id
    INNER JOIN Title ON Title_Genre.title = Title.id
    WHERE name LIKE '%Gill%'
        AND Genre.genre LIKE 'Talk-Show'
        AND  startYear = 2017
    GROUP BY name
    HAVING COUNT (name)=(
        SELECT MAX(tmp.c)
        FROM
            (SELECT COUNT(name) c
             FROM Member
                INNER JOIN Title_Producer ON Title_Producer.producer = Member.id
                INNER JOIN Title_Genre ON Title_Genre.title = Title_Producer.title
                INNER JOIN Genre ON Title_Genre.genre = Genre.id
                INNER JOIN Title ON Title_Genre.title = Title.id
                WHERE name LIKE '%Gill%'
                AND Genre.genre LIKE 'Talk-Show'
                AND  startYear = 2017
                GROUP BY name) tmp);

/*2.4*/
/*
3.4

This query starts with an aggregation on member.name. From here the plan branches into 2 paths. The first is an
aggregation, then another aggregation over member.name. After this, it performs a gather merge, then another aggregation
on member.name, then a sort on member.name. From here it goes into a nested loop where the paths branch, one of these
branches is an index scan on the primary key of the member table. The other branch is a hash join on the id of the title
table and the title part of the title_producer table. From here it splits into a sequential scan on the title_producer
table and a hash then sequential scan on the title field of the title table.

The other branch off the original starts with a gather merge, then an aggregation and sort on member.name. From this
point, we perform the inner join using a nested loop. This splits into an index scan on the primary key of the member
table. The other side of the loop is a hash join on the title part of title_producer and the id in the title table. At
this point it splits again into a sequential scan on the title_producer table and a hash then sequential scan on the
title table.
*/
SELECT name
    FROM Member
    INNER JOIN Title_Producer ON Title_Producer.producer = Member.id
    INNER JOIN Title ON Title.id = Title_Producer.title
    WHERE deathYear IS NOT NULL
        AND runtimeMinutes > 120
    GROUP BY name
    HAVING COUNT (name)=(
        SELECT MAX(tmp.c)
        FROM
            (SELECT COUNT(name) c
             FROM Member
                INNER JOIN Title_Producer ON Title_Producer.producer = Member.id
                INNER JOIN Title ON Title.id = Title_Producer.title
                WHERE deathYear IS NOT NULL
                    AND runtimeMinutes > 120
                GROUP BY name) tmp);
	
/*2.5*/
/*
3.5

This query starts with a gather. It then splits into 3 branches which I will explain in order. The first branch performs
a gather, then a sequential scan on the role table. The second branch does the same thing on another version of the
role table for the other subquery. The third branch leads to a nested loop which then splits into 2 branches. The one
branch is a sequential scan on the actor_title_role table. The other is an index scan on the primary key of the member
table
*/
SELECT name
    FROM Member
    INNER JOIN Actor_Title_Role ON Actor_Title_Role.actor = member.id
    WHERE deathYear IS NOT NULL
    AND (role = (SELECT id FROM Role  WHERE role LIKE '[Jesus]')
        OR  role = (SELECT id FROM Role WHERE role LIKE '[Christ]'));


-- Initial Times:
-- 2.1) It took  2.592266844s to run this query
-- 2.2) It took  3.097466077s to run this query
-- 2.3) It took  447.627472ms to run this query
-- 2.4) It took  2.519199152s to run this query
-- 2.5) It took  1.171576767s to run this query
