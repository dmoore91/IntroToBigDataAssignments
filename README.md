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
	* Instead of performing milionmasters of I/O events like I was prevously. I write all the data to a file as we process it and then once we're done processing it, read it in using the Postgres COPY command. This is exponentially faster then the previous way I was doing it because we have on the magnitude of millions less I/O/ events to have to deal with the over head of
* Three	
	* Wrote an init_db.sql to make a transaction to create all the tables for the schema
	* Reads in data much like I did in assignment two. Slight modifications to account for us writing 1 combined table as opposed to 10 separate tables
	* Implement prune functional dependency search to efficiently find functional dependencies
		* Runs in about 3 seconds
	* Implement naive functional dependency search to demonstrate how long it takes to brute force functional dependencies
		* I didn't let this run all the way through but it'll take about 8-9 days to complete. This is because it's intentionally inefficient to demonstrate why brute finding functional dependencies is a terrible idea
* Four
	* Wrote loadData.go to read data from assignement_two SQL schema into MongoDB schema.
	* Implement queries in Mongo to perform queries requested in assignment
	* Analyzed Mongo queries and add indices to speed up queries

* Five
	* Wrote createPreviousSources to create previous sources as view over assignment_two data
	* Wrote gav.go to create global view schemas over previous sources
	* Implemented queries via global views, ran and timed them
	* Optimized queries by going to previous sources and removing redundant joins and sources. Ran and timed these as well.
		* Longest query dropped from 1:03 to 26 seconds. Not a lot time wise but about 50% percentage wise
* Six
	* Read in data from provided JSON. Has to restructure a little bit because IDs in JSON are the IMDB id so I had to rework my IDs a bit to make sure that I could derive them from the IMDB ids and make it all fit together
	* Wrote optimized Go code that uses Goroutines to use up all potential processor power in order to show that using titles, which is the only other reasonably unique field, we can only get about 13000 unique documents out of several hundered thousand.
	* Wrote MongoDB queries using aggregation pipeline, then used the gonum plotter to plot a bar chart, box and whisker plot and time series graph as requested.
