from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder.getOrCreate()

sc = SparkContext(appName="Assignment2")
sc.setLogLevel("ERROR")

#import files
df = spark.read.json('/data/doina/Twitter-Archive.org/2020-01/01/00/0*.json.bz2') 
#spark.read.json accepts regular expressions like the one just above with *
# pyspark.sql.functions
df1 = df.select(col('entities')['hashtags'].alias('hashtags'))
df2 = df1.withColumn("hashtags", explode("hashtags")).select(col("hashtags")['text'].alias('hashtags'))
df3 = df2.groupBy('hashtags').count().orderBy('count', ascending = False).show().encode(’utf-8’)

