import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from math import log
from operator import add


# Step 1
def mapper_get_words(doc):
        sentence_list = doc.split()
        docID = sentence_list[0]
        sentence = sentence_list[1:]
        sentence_length = len(sentence)
        for word in sentence:
            yield  (word, docID, sentence_length), 1

def reducer_add(count1, count2):
  return count1 + count2

def reduce_list2(arr):
  new_arr = list(arr)
  for index, wordtuple in enumerate(new_arr):
    new_arr[index] = (wordtuple[0],  ((wordtuple[2]/wordtuple[1])*  (log(num_of_docs/ float(len(arr)) ))  )   )
  return new_arr

# Filtering out word that dont match gene_xyz_gene pattern
def find_word_pattern(row):
  word = row[0]
  word_list = word.split("_")
  lengthlist = len(word_list)
  if word_list[0] == "gene" and word_list[lengthlist -1] == "gene":
    return row

# Calculate cosine Pairs

def cosSimilarity(list1, list2):
  return dotProduct(list1, list2) / productOfNorms(list1,list2)


def dotProduct(list1, list2):
  return  sum([x[1]*y[1] for x,y in zip(list1,list2)])

def productOfNorms(list1,list2):
  return getNorm(list1) * getNorm(list2)

def getNorm(some_list):
  ans = 0
  for item in some_list:
    num = item[1] ** 2
    ans = ans + num
  return ans ** .5

if __name__ == "__main__":
  findspark.init()
  sc = SparkContext(appName="MyFirstApp")
  spark = SparkSession(sc)


  rdd = sc.textFile("test.txt")
  # Total number of docs in file
  num_of_docs = rdd.count()

  rdd1 = rdd.flatMap(mapper_get_words)
  rdd2 = rdd1.reduceByKey(reducer_add)

  rdd3 = rdd2.map(lambda word: (word[0][0], (word[0][1], word[0][2], word[1])) )

  rdd4 = rdd3.groupByKey().mapValues(lambda x: reduce_list2(x))

  rdd5 = rdd4.filter(find_word_pattern)

  termPairs = rdd5.cartesian(rdd5).filter(lambda x: x[0][0] > x[1][0])

  cosSimPairs = termPairs.map(lambda pair: (  cosSimilarity(pair[0][1], pair[1][1]) , (pair[0][0], pair[1][0])     )    )

  final = cosSimPairs.sortByKey(ascending=False)
  # Top 5 Results
  top5results = final.take(5)
  for r in top5results:
    print(r)
