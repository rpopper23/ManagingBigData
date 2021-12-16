'''
Ruben Popper: s2882930
real	0m15.214s
user	0m50.745s
sys	0m2.988s
'''
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split
sc = SparkContext(appName = 'Assignment')
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")
#import files into dataframe
df = spark.read.text('/data/doina/integers.txt')
#explode the list contained in each row to obtain a row value for each item in every list
df1 = df.select(split(col("value"),",").alias('value')).withColumn("value", explode('value'))
#order the row values and set the number of partition to 10
df2 = df1.orderBy('value').repartition(10)
#convert to rdd to save as text
df1.rdd.saveAsTextFile('MBD_Assignments/DSORT_Results')

####### PATH TO FOLDER WITH TEXT FILES #######
#/user/s2882930/ManagingBigData/Assignment2/sort_s288293
