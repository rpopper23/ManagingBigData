'''
Ruben Popper: s2882930
RunTime with different max number of executors reported below
'''
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
# needed for encoding
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
#set up spark environment for py.script
sc = SparkContext(appName = 'Assignment')
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")
#import files
df = spark.read.json('/data/doina/Twitter-Archive.org/2020-01/01/00/0*.json.bz2')
#create table containing hashtags column
df1 = df.select(col('entities')['hashtags'].alias('hashtags'))
#convert dataframe entries to rows and select those containing the hashtags text
df2 = df1.withColumn("hashtags", explode("hashtags")).select(col("hashtags")['text'].alias('hashtags'))
#for each distinct hashtags, count its occurences and sort in descending order
df3 = df2.groupBy('hashtags').count().orderBy('count', ascending = False)
#save results on HDFS
df3.write.format('json').save('MBD_Assignments/HASHTAGS_Results')

'''
1
real	1m36.246s
user	0m15.370s
sys	0m2.512s
2 
real	1m54.686s
user	0m15.283s
sys	0m2.421s

3
real	1m34.755s
user	0m13.842s
sys	0m2.437s

4
real	1m20.512s
user	0m14.496s
sys	0m2.411s

5
real	1m52.879s
user	0m12.830s
sys	0m2.455s

6
real	1m21.974s
user	0m13.600s
sys	0m2.407s
81.97, 86.97, 90.41, 94.04, 93.89

7
real	1m26.972s
user	0m13.220s
sys	0m2.701s

8
real	1m30.306s
user	0m14.730s
sys	0m2.639s

9
real	1m34.038s
user	0m13.731s
sys	0m2.607s

10
real	1m33.890s
user	0m13.541s
sys	0m2.643s
'''