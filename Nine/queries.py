from pyspark.sql import SparkSession
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


if __name__ == '__main__':
    phi_actors()
