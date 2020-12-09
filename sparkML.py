import sys
from pyspark.sql import SparkSession, functions as f
from pyspark.ml.feature import HashingTF
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator

cassandra_host = 'localhost'


def main(mode):
    raw = spark.read.format("org.apache.spark.sql.cassandra") \
		.options(table="emails", keyspace"email_database").load()
    
    hashingTransformer = HashingTF(inputCol='word_tokens', outputCol='features')
    hashingTransformer.setNumFeatures(1000)
    SVC = LinearSVC(featuresCol='features', labelCol='labels', predictionCol='prediction')
    SVC_pipeline = Pipeline(states=[hashingTransformer, LinearSVC])

    train, validation = raw.randomSplit([0.75, 0.25])
    train = train.cache()
	validation = validation.cache()
    SVC_model = SVC_pipeline.fit(train)
    prediction = SVC_model.transform(balidation)


if__name__ == 'main':
    spark = SparkSession.builder.appName('emailML')\
		.config('spark.cassandra.connection.host', cassandra_host).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
	main()
