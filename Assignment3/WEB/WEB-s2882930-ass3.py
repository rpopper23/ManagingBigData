from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
sc = SparkContext(appName="ass2")
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")
#import files
df = spark.read.json('/data/doina/WebInsight/2020-07-13/')
df1 = spark.read.json('/data/doina/WebInsight/2020-09-14/')

df = df.select(col('url'), col('fetch')['textSize'].alias('textSize'))
df1 = df1.select(col('url'), col('fetch')['textSize'].alias('textSize1'))

cond = [df.url == df1.url, (df.textSize > 0) | (df1.textSize1 > 0)]

join_df = df.join(df1, cond, 'inner').select(df.url, (df1.textSize1 - df.textSize).alias("sizediff")).orderBy('sizediff')
join_df.coalesce(8).write.format('json').save('MBD_Assignments/WEB_Results')
 

# /user/s2882930/MBD_Assignments/WEB_Results