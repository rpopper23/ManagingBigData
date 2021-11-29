
from pyspark import SparkContext
sc = SparkContext(appName="Assignment") #terminal does this automatically
sc.setLogLevel("ERROR")
rdd1 = sc.wholeTextFiles("/data/doina/Gutenberg-EBooks")
rdd2 = rdd1.map(lambda(path, content): (path, content.lower().split()))
rdd3 = rdd2.flatMap(lambda (path,content):((word, [path]) for word in set(content)))
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)
rdd_output =  rdd4.sortByKey(ascending=True).filter(lambda x: len(x[1])>=13)
print(rdd_output.keys().collect())
