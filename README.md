# Depedencies

- python
- mrjob
- mr3px
- pyspark
- findspark

# Running

Run either using make run OR python3 TF_IDF.py test.txt >> test.csv

- test.csv is the output of the docID, word, and tf-idf score

The next script (spark-test.py) loads the csv into a dataframe and converts that dataframe into a m x n matrix (word x docID)

# PySpark/Spark setup

- Must download spark and set the SPARK_HOME environment variable to the path to where your spark download is. Look up tutorials for more information and instruction
