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

def mapper_word_freq(word):
    termCountForDoc = word[1]
    term = word[0][0]
    docID = word[0][1]
    docLength = word[0][2]
    yield (term, 1)

def reducer_word_freq_corpus(word1,  word2):
    return word1 + word2

# Step 3 (("'active'", 'doc7300', 143), 1)
def map(word):
  term = word[0][0]
  docID = word[0][1]
  docLength = word[0][2]
  termCountForDoc = word[1]
  yield (term, [docID, docLength, termCountForDoc])


def map2(word):
  term = word[0]
  docID = word[1][0][0]
  docLength = word[1][0][1]
  termCountForDoc = word[1][0][2]
  termCountForAllDocs = word[1][1]

  tfidf = (termCountForDoc / docLength) * log(num_of_docs/ termCountForAllDocs)

  yield (term, docID), tfidf


findspark.init()
sc = SparkContext(appName="MyFirstApp")
spark = SparkSession(sc)


rdd = sc.textFile("test.txt")

num_of_docs = rdd.count()
print(num_of_docs)

rdd1 = rdd.flatMap(mapper_get_words)
rdd1 = rdd1.sortByKey()

rdd2 = rdd1.reduceByKey(reducer_add)
rdd2 = rdd2.sortByKey()

rdd3 = rdd2.flatMap(mapper_word_freq)
rdd3 = rdd3.sortByKey()

rdd4 = rdd3.reduceByKey(reducer_word_freq_corpus)

rdd5 = rdd2.flatMap(map)

rdd6 = rdd5.join(rdd4)

rdd7 = rdd6.flatMap(map2)
print(rdd7.take(100))
