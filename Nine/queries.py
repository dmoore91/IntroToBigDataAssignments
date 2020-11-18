from pyspark.sql import SparkSession
from pyspark.sql.functions import split, array_contains
import pyspark.sql.functions as f
import time


def phi_actors():

    start = time.time()

    spark = SparkSession.builder.master("local").appName("Phi").getOrCreate()

    titles = spark.read.csv("title.basics.tsv", sep="\t", header=True)
    people = spark.read.csv("name.basics.tsv", sep="\t", nullValue="\\N", header=True)
    principals = spark.read.csv("title.principals.tsv", sep="\t", header=True)
    actors = principals.filter(principals.category == 'actor')

    title_to_actor = titles.join(actors, on=['tconst'], how='inner')
    title_to_actor = title_to_actor.join(people, on=['nconst'], how='inner')

    phis = title_to_actor.filter(title_to_actor.primaryName.startswith("Phi"))
    living_phis = phis.filter(phis.deathYear.isNull())

    acted_in_2014 = living_phis.filter(living_phis.startYear == 2014)

    didntactin2014 = living_phis.exceptAll(acted_in_2014)

    names = didntactin2014[['primaryName']]
    unique_names = names.dropDuplicates()

    print("Took: " + str(time.time() - start) + " seconds")

    for row in unique_names.head(10):
        print(row)


def prolific_gills():
    start = time.time()

    spark = SparkSession.builder.master("local").appName("Gil").getOrCreate()

    titles = spark.read.csv("title.basics.tsv", sep="\t", header=True)
    people = spark.read.csv("name.basics.tsv", sep="\t", nullValue="\\N", header=True)
    principals = spark.read.csv("title.principals.tsv", sep="\t", header=True)
    producers = principals.filter(principals.category == 'producer')

    titles = titles.withColumn('genres',  split(titles['genres'], ","))
    title_to_producer = titles.join(producers, on=['tconst'], how='inner')
    title_to_producer = title_to_producer.join(people, on=['nconst'], how='inner')
    title_to_producer = title_to_producer.filter(array_contains(title_to_producer.genres, "Talk-Show"))
    title_to_producer = title_to_producer.filter(title_to_producer.startYear == 2017)

    gills = title_to_producer.filter(title_to_producer.primaryName.contains("Gill"))

    counts = gills.groupBy("primaryName").agg(f.countDistinct("primaryTitle").alias("count"))

    max_count = counts.orderBy('count', ascending=False).head(1)[0]['count']

    prolific_producers = counts.filter(f.col('count') == max_count)

    print("Took: " + str(time.time() - start) + " seconds")

    for row in prolific_producers.head(10):
        print(row)


def longRunningProducers():

    start = time.time()

    spark = SparkSession.builder.master("local").appName("Gil").getOrCreate()

    titles = spark.read.csv("title.basics.tsv", sep="\t", header=True)
    people = spark.read.csv("name.basics.tsv", sep="\t", nullValue="\\N", header=True)
    principals = spark.read.csv("title.principals.tsv", sep="\t", header=True)
    producers = principals.filter(principals.category == 'producer')

    titles = titles.withColumn('genres', split(titles['genres'], ","))
    title_to_producer = titles.join(producers, on=['tconst'], how='inner')
    title_to_producer = title_to_producer.join(people, on=['nconst'], how='inner')
    title_to_producer = title_to_producer.filter(title_to_producer.deathYear.isNull())

    long_running = title_to_producer.filter(title_to_producer.runtimeMinutes > 120)

    counts = long_running.groupBy("primaryName").agg(f.countDistinct("primaryTitle").alias("count"))

    max_count = counts.orderBy('count', ascending=False).head(1)[0]['count']

    prolific_producers = counts.filter(f.col('count') == max_count)

    print("Took: " + str(time.time() - start) + " seconds")

    for row in prolific_producers.head(10):
        print(row)


def jesusChristActors():
    start = time.time()

    spark = SparkSession.builder.master("local").appName("Phi").getOrCreate()

    people = spark.read.csv("name.basics.tsv", sep="\t", nullValue="\\N", header=True)
    principals = spark.read.csv("title.principals.tsv", sep="\t", header=True)
    actors = principals.filter(principals.category == 'actor')

    actor_to_role = people.join(actors, on=['nconst'], how='inner')
    actor_to_role = actor_to_role.filter(actor_to_role.deathYear.isNull())

    jesus = actor_to_role.filter(actor_to_role.characters.contains("Jesus"))
    christ = actor_to_role.filter(actor_to_role.characters.contains("Christ"))

    u = jesus.unionAll(christ)

    names = u[['primaryName']]
    unique_names = names.dropDuplicates()

    print("Took: " + str(time.time() - start) + " seconds")

    for row in unique_names.head(10):
        print(row)


if __name__ == '__main__':
    # phi_actors()
    # prolific_gills()
    # longRunningProducers()
    jesusChristActors()
