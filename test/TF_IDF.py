from mrjob.job import MRJob #pip3 install mrjob
from mrjob.step import MRStep
from mr3px.csvprotocol import CsvProtocol # pip install mr3px
from math import log
import csv


# pip3 install findspark
# pip3 install pyspark

# RUN USING python3 TF_IDF.py test.txt >> test.csv

class TD_IDF(MRJob):

    # OUTPUT_PROTOCOL = CsvProtocol

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.word_count_for_sentence),
            MRStep(mapper=self.mapper_word_freq,
                reducer=self.reducer_word_freq),
            MRStep(mapper=self.mapper_word_freq_doc,
                reducer=self.reducer_word_freq_doc),
            MRStep(mapper=self.mapper_td_idf)

        ]

    def mapper_get_words(self, _, doc):
        sentence_list = doc.split(" ")
        docID = sentence_list[0]
        sentence = sentence_list[1:]
        sentence_length = len(sentence_list)
        for word in sentence:
            yield (word, docID, sentence_length), 1
        # for word in sentence_list:
        #     yield (word, sentence_length, doc), 1

    # Computing Term Frequency
    def word_count_for_sentence(self, word, count):
        yield (word[0], word[1], word[2]), sum(count)

    # Step 2 - Word Freq For each Doc

    def mapper_word_freq(self, word, count):
        yield word[1], (word[0],count, word[2])

    def reducer_word_freq(self, docID, wordCount):
        total = 0
        n = []
        D = []
        word = []
        wordCount = list(wordCount)
        for value in wordCount:
            total += value[1]
            n.append(value[1])
            word.append(value[0])
            D.append(value[2])
        N = [total]*len(word)
        # print("{} - {}".format(docID,total))
        for value in range(len(word)):
            yield (word[value], docID, D[value]), (n[value], N[value])
            # n = value[1]
            # word = value[0]
        # yield (wordCount[0],docID),(wordCount[1],N)

    # Step 3 - Word Freq for whole Doc
    def mapper_word_freq_doc(self, wordDocID,  countFreq):
        yield wordDocID[0], (wordDocID[1], countFreq[0], countFreq[1], wordDocID[2], 1)

    def reducer_word_freq_doc(self, word, wordCountFreq):
        total = 0
        docName = []
        n = []
        N = []
        D = []
        wordCountFreq = list(wordCountFreq)
        for value in wordCountFreq:
            total += 1
            docName.append(value[0])
            n.append(value[1])
            N.append(value[2])
            D.append(value[3])

        # we need to compute the total numbers of documents in corpus
        m = [total]* len(n)

        for value in range(len(m)):
            yield (word, docName[value], D[value]), (n[value], N[value], m[value])

    # Step 4 - Compute TF-IDF
    def mapper_td_idf(self, wordData, wordNums):
        tf_idf = (wordNums[0] / wordNums[1]) * log(wordData[2] / wordNums[2])
        # print(type(wordData[0]))
        # print(type(wordData[1]))
        # print(type(tf_idf))
        # yield ( (wordData[0], wordData[1] ), str(tf_idf) )
        data_list = [wordData[1], wordData[0] , str(tf_idf)]
        yield (None, data_list)


if __name__ == '__main__':
    # f = open("test.csv", "a+")
    # writer = csv.writer(f)
    # writer.writerow(["id", "word", "tf-idf"])
    TD_IDF.run()
