'''
Ruben Popper: s2882930
real	0m9.291s
user	0m13.559s
sys	0m1.293s
'''
#set up spark environment for py.script
from pyspark import SparkContext
sc = SparkContext(appName="Assignment") #terminal does this automatically
sc.setLogLevel("ERROR")
#read file
rdd1 = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")
#extract single word withn each document
rdd2 = rdd1.map(lambda(path, content): (path, content.lower().split()))
#associate document path names to single word tokens
rdd3 = rdd2.flatMap(lambda(path, content):((word, [path]) for word in set(content)))
#link a word to each documents where it appears
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
#filter the results for words appearing in more than 13 documents
rdd_output =  rdd4.sortByKey(ascending=True).filter(lambda x: len(x[1])>=13)
print(rdd_output.keys().collect())


