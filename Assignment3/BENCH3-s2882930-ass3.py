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
#save
df3.write.format('json').save('file:///home/s2882930/ManagingBigData/Assignment3/WEB/WEB_Results')

df3.write.format('json').save('file:///user/s2882930/ManagingBigData/Assignment3/BENCH_Results')
