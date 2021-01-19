from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("US Congress Members terms").getOrCreate()

data_folder = 'sourcefiles/'

file_path = data_folder + "congress-terms.csv"

congress = spark.read.format("csv").option('head','true').option("inferSchema", "true").load(file_path)

congress = congress.withColumnRenamed("_c0", "Congress") \
       .withColumnRenamed("_c1", "Chamber") \
       .withColumnRenamed("_c2", "Bioguide") \
       .withColumnRenamed("_c3", "FirstName") \
       .withColumnRenamed("_c4", "MiddleName") \
       .withColumnRenamed("_c5", "LastName") \
       .withColumnRenamed("_c6", "Suffix") \
       .withColumnRenamed("_c7", "BirthDate") \
       .withColumnRenamed("_c8", "State") \
       .withColumnRenamed("_c9", "Party") \
       .withColumnRenamed("_c10", "Incumbent") \
       .withColumnRenamed("_c11", "TermStart") \
       .withColumnRenamed("_c12", "Age")

congress.printSchema()
congress.select("State","FirstName","LastName","Party").where(congress.State == "CA").show(30)

congress.select("FirstName", "LastName", "State", "Incumbent").where(congress.Incumbent == "Yes")

spark.stop()