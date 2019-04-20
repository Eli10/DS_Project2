run:
	rm test.csv
	pip install mrjob
	pip install mr3px
	pip install findspark
	pip install pyspark
	python3 TF_IDF.py test.txt > test.csv
