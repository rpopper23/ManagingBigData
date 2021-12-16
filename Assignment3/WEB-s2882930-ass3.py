from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
sc = SparkContext(appName="ass2")
spark = SparkSession.builder.getOrCreate()
sc.setLogLevel("ERROR")
#import files
df = spark.read.json('/data/doina/WebInsight/2020-07-13/1M.2020-07-13-aa.gz')
df1 = spark.read.json('/data/doina/WebInsight/2020-09-14/1M.2020-09-14-aa.gz')

df = df.select(col('url'), col('fetch')['textSize'].alias('textSize'))
df1 = df1.select(col('url'), col('fetch')['textSize'].alias('textSize1'))

#df2 = df.join(df1, df.url == df1.url, "inner")


#cond = [df.url == df1.url, df1.textSize1 != 0]
#join_df = df.join(df1, cond, 'inner').select(df.url, df.textSize, df1.textSize1)

join_df = df.join(df1, cond, 'inner').select(df.url, (df.textSize - df1.textSize1).alias("sizediff")).orderBy('sizediff')

#join_df.coalesce(1).write.format('json').save('/path/file_name.json')



###using sql
#join_df = spark.sql('select df1.url, df.textSize, df1.textSize from df inner join df1 on df.url=df1.url where df1.textSize1>0')
#(df1.textSize1 - df.textSize) as "difference"


