from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
sc = SparkContext(appName="ass2")
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")
#import files
df = spark.read.json('/data/doina/UCSD-Amazon-Data/meta_Video_Games.json.gz')
# see the first product in the file : hdfs dfs -text /data/doina/UCSD-Amazon-Data/meta_Video_Games.json.gz | head -1 
#within the struct column 'related', select the element of interest (the array containin 'also_bought' items)
df1 = df.select(col('related')['also_bought'].alias('also_bought'))
#explode the array to obtain row representation of each also_bought item
df2 = df1.withColumn("also_bought", explode("also_bought"))
#take distinct products and count their occurences, sort them in descending order based on their count
top = df2.groupBy('also_bought').count().orderBy('count', ascending = False)
#From the original dataframe, retrieve other information about the top item obtained in the previous step
df.filter(col("asin") == top.collect()[0][0]).select(col('brand'), col('description'), col('price'), col('title'), col('imUrl'), col('salesRank'), col('categories')).show(truncate = False, vertical = True)
