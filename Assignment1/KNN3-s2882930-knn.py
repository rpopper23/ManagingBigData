'''
Ruben Popper: s2882930
real	0m11.568s
user	0m14.304s
sys	0m1.315s
'''
#set up spark environment for py.script
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
from pyspark.ml.feature import VectorAssembler
from math import sqrt
sc = SparkContext(appName="Assignment")#terminal does this automatically
spark = SparkSession(sc)
sc.setLogLevel("ERROR")
#define function for euclidean distance
def euclidean_distance(row1, row2):
        distance = 0.0
        for i in range(len(row1)-1):
                distance += (row1[i] - row2[i])**2
        return sqrt(distance)
#hard-code testpoint
given_point = (100,100)
given_k=100
#import data in RDD object
rdd_data = sc.textFile('/data/doina/xyvalue.csv')
#Preprocess numeric values for each datapoint
rdd_xyv = rdd_data.flatMap(lambda content: [[float(val) for val in content.split(',')]])
#calculate test point distance from other observations
rdd_value_distances = rdd_xyv.map(lambda xyv: (xyv[0], xyv[1], xyv[2], euclidean_distance(xyv, given_point)))
#order observations based on distance take the 100 data points closer to test point
list_top_k = rdd_value_distances.takeOrdered(given_k, key = lambda tuple: tuple[3])
#Compute their average
avg_value = 0
for tup in list_top_k:
    avg_value += tup[2]
print(avg_value/given_k)
