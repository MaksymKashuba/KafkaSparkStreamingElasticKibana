from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName('KafkaSparkStreaming') \
    .master('local[*]') \
    .config('spark.jars.packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'
            ',org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3') \
    .getOrCreate()

schema = StructType([StructField('speaker', StringType(), True),
                     StructField('time', TimestampType(), True),
                     StructField('word', StringType(), True)])
kafkaBrokerHost = "localhost:9092"
kafkaTopic = 'KafkaHW'
censored = 'C:/Users/max19/PycharmProjects/KafkaHW/censored.txt'
windowDuration = '10 minutes'
censoredLit = '***'
getKafka = spark.readStream\
    .format('kafka') \
    .option("kafka.bootstrap.servers", kafkaBrokerHost) \
    .option("startingOffsets", "earliest") \
    .option('subscribe', kafkaTopic).load()
getKafka.printSchema()
strDf = getKafka.withColumn('value', getKafka['value'].cast(StringType()))
finDf = strDf.select(from_json(col('value'), schema).alias('data')).select('data.*')
censoredList = [x.replace('\n', '') for x in open(censored).readlines()]
for i in censoredList:
    finDf = finDf.withColumn('word', when(col('word').rlike(i), lit(censoredLit)).otherwise(col('word')))
finDf.createOrReplaceTempView("Data")
df1 = finDf\
    .withWatermark('time', '10 minutes')\
    .groupBy('speaker', window(col('time'), '10 minutes'))\
    .agg(collect_set('word')).withColumnRenamed('collect_set(word)', 'words').select('window', 'words', 'speaker')
query = df1.writeStream\
    .outputMode("append") \
    .queryName("writing_to_es") \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/") \
    .option("es.port", "9200") \
    .option("es.nodes", "127.0.0.1") \
    .option("es.index.auto.create", "true") \
    .option("es.nodes.wan.only", "true") \
    .start("doc")
query.awaitTermination()

