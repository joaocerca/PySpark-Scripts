import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
from pyspark.sql.functions import when
import numpy as np  # type: ignore[import]
import pandas as pd  # type: ignore[import]


# File location and type
file_location = "sourcefiles/football_stats/sresults.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

spark = SparkSession.builder \
    .master("local") \
    .config("spark.sql.autoBroadcastJoinThreshold", -1) \
    .appName("FootballStats") \
    .getOrCreate()

spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# The applied options are for CSV files. For other file types, these will be ignored.
footballResults = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)

# footballResults_pd = footballResults.select("*").toPandas()


# print(footballResults)

scotlandResults = footballResults.select("*").where((col("home_team") == "Scotland") | (col("away_team") == "Scotland"))
totalMatches = scotlandResults.count()

print("Total number of matches played by Scotland: {}".format(totalMatches))
print("Total number of official matches played by Scotland: {}".format(scotlandResults.where(col("tournament") != "Friendly").count()))
print("Total number of friendly matches played by Scotland: {}".format(scotlandResults.where(col("tournament") == "Friendly").count()))

# Total results of Scotland (official and friendly)
scotlandResults2 = scotlandResults.select("*", F.when(((col("home_score") > col("away_score")) & (col("country") == "Scotland")), "Sct. Won")
                             .when(((col("home_score") > col("away_score")) & (col("country") != "Scotland")), "Sct. Lost")
                             .when(((col("home_score") < col("away_score")) & (col("country") == "Scotland")), "Sct. Lost")
                             .when(((col("home_score") < col("away_score")) & (col("country") != "Scotland")), "Sct. Won")
                             .otherwise("Draw").alias("results"))

# Total official results of Scotland
scotlandResults3 = scotlandResults.select("*", F.when(((col("home_score") > col("away_score")) & (col("country") == "Scotland")), "Sct. Won")
                             .when(((col("home_score") > col("away_score")) & (col("country") != "Scotland")), "Sct. Lost")
                             .when(((col("home_score") < col("away_score")) & (col("country") == "Scotland")), "Sct. Lost")
                             .when(((col("home_score") < col("away_score")) & (col("country") != "Scotland")), "Sct. Won")
                             .otherwise("Draw").alias("results")).where((col("tournament") != "Friendly"))


numHomeMatches = scotlandResults3.select("*").where(col("home_team") == "Scotland").count()
numAwayMatches = scotlandResults3.select("*").where(col("home_team") != "Scotland").count()
numLostHomeMatches = scotlandResults3.select("*").where((col("home_team") == "Scotland") & (col("results") == "Sct. Lost")).count()
numWonHomeMatches = scotlandResults3.select("*").where((col("home_team") == "Scotland") & (col("results") == "Sct. Won")).count()
numLostAwayMatches = scotlandResults3.select("*").where((col("away_team") == "Scotland") & (col("results") == "Sct. Lost")).count()
numWonAwayMatches = scotlandResults3.select("*").where((col("away_team") == "Scotland") & (col("results") == "Sct. Won")).count()
numDrawHomeMatches = scotlandResults3.select("*").where((col("home_team") == "Scotland") & (col("results") == "Draw")).count()
numDrawAwayMatches = scotlandResults3.select("*").where((col("away_team") == "Scotland") & (col("results") == "Draw")).count()

print("Scotland played {}".format(numHomeMatches), "official home matches. Won {}".format(numWonHomeMatches), ", lost {} and".format(numLostHomeMatches), "{} where the result was a draw.".format(numDrawHomeMatches))
print("Scotland played {}".format(numAwayMatches), "official away matches. Won {}".format(numWonAwayMatches), ", lost {} and".format(numLostAwayMatches), "{} where the result was a draw.".format(numDrawAwayMatches))


# Calculation of goal difference for winning matches
scotlandResults_won = scotlandResults3.select("*").where(col("results") == "Sct. Won").withColumn('diff', F.abs(col("home_score") - col("away_score")))

print("\nTop 10 official Scottish winning matches with the highest goal difference")
scotlandResults_won.select('date', 'home_team', 'away_team', 'home_score', 'away_score', 'diff', 'tournament', 'city', 'results').sort(col("diff").desc()).show(10)

# Calculation of goal difference for losing matches
scotlandResults_lost = scotlandResults3.select("*").where(col("results") == "Sct. Lost").withColumn('diff', F.abs(col("home_score") - col("away_score")))

print("\nTop 10 official Scottish losing matches with the highest goal difference")
scotlandResults_lost.select('date', 'home_team', 'away_team', 'home_score', 'away_score', 'diff', 'tournament', 'city', 'results').sort(col("diff").desc()).show(10)


print("\nTop 10 list of cities where Scotland won official matches")
scotlandResults_won.groupby("city").count().sort(col("count").desc()).withColumnRenamed("count", "No. of matches").show(10)

print("\nTop 10 list of cities where Scotland lost official matches")
scotlandResults_lost.groupby("city").count().sort(col("count").desc()).withColumnRenamed("count", "No. of matches").show(10)



