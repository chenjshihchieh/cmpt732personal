import sys
from pyspark.sql import SparkSession, functions as f, types





def main():
	email_schema = types.StructType([
		types.StructField('wholetext', StringType()),
	])
	
	raw_email = spark.read.text(**textsource**, wholetext = true)

if __name__ == '__main__':
    spark = SparkSession.builder.appName('email ETL').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main()