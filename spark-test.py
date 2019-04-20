import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F




findspark.init()
sc = SparkContext(appName="MyFirstApp")
spark = SparkSession(sc)
print("Hello World!")

df = spark.read.csv("test.csv", header=False)
# df.show()
df = df.toDF('id', 'word', 'tfidf')
df = df.withColumn("tfidf", df.tfidf.cast("double"))
df.show()

# Reshaping data into MxN Matrix (Word x Document_ID)
reshaped_df = df.groupby('word').pivot('id').max('tfidf').fillna(0)

reshaped_df.printSchema()

# Showing one row from matrix for a wordData
reshaped_df.where(reshaped_df.word == 'mechanism').collect()
