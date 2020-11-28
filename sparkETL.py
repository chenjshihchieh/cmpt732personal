import sys
from pyspark.sql import SparkSession, functions as f, types

sender_regex = r'From:[^<]+<([^\n]+)>'
receiver_regex = r'To: ([^\n]+)'
subject_regex = r'Subject: ([^\n]+)'
body_regex = r'\n\n([\H\n\s]*)'



def main(inputs, output):
	raw_email = spark.read.text(inputs, wholetext = True)
	send_to_cassandra = raw_email.select(\
		f.regexp_extract('value', sender_regex, 1).alias('sender'), \
		f.regexp_extract('value', receiver_regex, 1).alias('receiver'), \
		f.regexp_extract('value', subject_regex, 1).alias('subject'), \
		f.regexp_extract('value', body_regex, 1).alias('body'))\
	.withColumn('id', f.monotonically_increasing_id())
	
	send_to_cassandra.write.csv(output, mode='overwrite')
	
	send_to_cassandra.write.format("org.apache.spark.sql.cassandra")\
		.options(table = 'email', keyspace = 'email_database').save()
	
	
	
	
	
	
	
	
	
if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('email ETL')\
		.config('spark.cassandra.connection.host', 'localhost').getOrCreate()
	spark.sparkContext.setLogLevel('WARN')
	main(inputs, output)
