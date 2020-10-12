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
	* Wrote an init_db.sql to make a transaction to create all the tables for the schema
	* Goroutines used to parallelize reading in, processing and writing to disk title and names. These two are run in parallel first becasuse they are not dependent on each other, but the other tables depend on them. Therefore I process them both in parralel and wait for them to finish before moving on to the other tables.
	* Next tables are split up and parallized so that tables that depend on other tables will not have to be run in parralel but tables that independent are handled in parralel. Essentially handling each "branch of the tree" separately.
	* Instead of performing milions of I/O events like I was prevously. I write all the data to a file as we process it and then once we're done processing it, read it in using the Postgres COPY command. This is exponentially faster then the previous way I was doing it because we have on the magnitude of millions less I/O/ events to have to deal with the over head of
* Three	
	* Wrote an init_db.sql to make a transaction to create all the tables for the schema
	* Reads in data much like I did in assignment two. Slight modifications to account for us writing 1 combined table as opposed to 10 separate tables
	* Implement prune functional dependency search to efficiently find functional dependencies
		* Runs in about 3 seconds
	* Implement naive functional dependency search to demonstrate how long it takes to brute force functional dependencies
		* I didn't let this run all the way through but it'll take about 8-9 days to complete. This is because it's intentionally inefficient to demonstrate why brute finding functional dependencies is a terrible idea
