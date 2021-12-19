'''
Ruben Popper: s2882930 
real	2m28.122s
user	0m12.791s
sys	0m2.599s
'''
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sc = SparkContext(appName="ass2")
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")
#import files
df = spark.read.json('/data/doina/WebInsight/2020-07-13/')
df1 = spark.read.json('/data/doina/WebInsight/2020-09-14/')

#select columns of interest
df = df.select(col('url'), col('fetch')['textSize'].alias('textSize'))
df1 = df1.select(col('url'), col('fetch')['textSize'].alias('textSize1'))

#define join conditions
cond = [df.url == df1.url, (df.textSize > 0) | (df1.textSize1 > 0)]

#perform join, create new column containing date difference, and order results by it
join_df = df.join(df1, cond, 'inner').select(df.url, (df1.textSize1 - df.textSize).alias("sizediff")).orderBy('sizediff')

#reduce number of partitions and save to HDFS
join_df.coalesce(8).write.format('json').save('MBD_Assignments/WEB_Results')
 
#Path:
# /user/s2882930/MBD_Assignments/WEB_Results
#Launch Command
# time spark-submit --master yarn --deploy-mode cluster --conf spark.dynamicAllocation.maxExecutors=10 WEB-s2882930-ass3.py
