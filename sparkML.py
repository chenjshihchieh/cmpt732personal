import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import HashingTF

cassandra_host = 'localhost'


def main(mode):
    raw = spark.read.format("org.apache.spark.sql.cassandra") \
		.options(table="emails", keyspace"email_database").load()
    
    hashingWords = HashingTF(inputCol='word_tokens', outputCol='features')
    hashingWords.setNumFeatures(1000)
    
##Create a list of all the words from all the email
    distinct_words = raw.withColumns('distinct', f.array_distinct(raw['word_tokens']))
    all_words_with_repeat = distinct_words.select(f.collect_set('distinct')).collect()
    all_words = []
    for sublist in all_words_with_repeat[0][0]:
        for item in sublist:
            all_words.append(item)
    words_set = set(all_words)


    train, validation = raw.randomSplit([0.75, 0.25])
    train = train.cache()
	validation = validation.cache()`
    n = 
    


if__name__ == 'main':
    spark = SparkSession.builder.appName('email ETL')\
		.config('spark.cassandra.connection.host', cassandra_host).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
	main()
