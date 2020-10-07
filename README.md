# IntroToBigDataAssignments

This repo is used to hold all my assignments for my Intro To Big Data course. I will describe each assignment in detail below, as well as some details about my implementation

# Assignments

* One
	* I developed an ER diagram to represent the IMDB data we had to read in.
	* Wrote an init_db.sql to make a transaction to create all the tables for the schema
	* Described the files and their contents of the IMDB data
	* Wrote a transaction with one commit that fails to show rollback feature of transaction
	* Wrote program to read in data. This one is written differently from the othr ones because I inserted every row of every table individually. Since this is roughly 70 million I/O events, this code is extremely slow and takes about 6.5hrs to run. I solved these problems for my later assignment and they are significantly faster, taking less then 10 minutes
* Two
