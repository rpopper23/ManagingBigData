'''
Ruben Popper: s2882930
real    3m2.895s
user    0m12.762s
sys     0m2.594s
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
    
df = df.select(col('url'), col('fetch')['textSize'].alias('textSize'))
df1 = df1.select(col('url'), col('fetch')['textSize'].alias('textSize1'))
    
cond = [df.url == df1.url, (df.textSize > 0) | (df1.textSize1 > 0)]
    
join_df = df.join(df1, cond, 'inner').select(df.url, (df1.textSize1 - df.textSize).alias("sizediff")).orderBy('sizediff')
                             
join_df.coalesce(8).write.format('json').save('file:///home/s2882930/ManagingBigData/Assignment3/WEB/WEB_Results')
