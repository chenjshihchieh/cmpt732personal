import sys
from simplified_scrapy.simplified_doc import SimplifiedDoc 
from pyspark.sql import SparkSession, functions as f, types

sender_regex = r'From:[^<]+<([^\n]+)>'
receiver_regex = r'To: ([^\n]+)'
subject_regex = r'Subject: ([^\n]+)'
body_regex = r'\n\n([\H\n\s]*)'
date_regex = r'Date: +([^\n]+)'
length_regex = r'Content-Length: +([^\n]+'
cassandra_host = 'localhost'

def simplify_doc_udf(html_text):
	doc = SimplifiedDoc(html_text)
	return doc.text

def main(inputs, output):
	html_to_plain_udf = f.udf(simplify_doc_udf, types.StringType())
	raw_email = spark.read.text(inputs, wholetext = True)
	send_to_cassandra = raw_email.select(\
		f.regexp_extract('value', sender_regex, 1).alias('sender'), \
		f.regexp_extract('value', receiver_regex, 1).alias('receiver'), \
		f.regexp_extract('value', subject_regex, 1).alias('subject'), \
		f.regexp_extract('value', date_regex, 1).alias('date'), \
		f.regexp_extract('value', length_regex, 1).alias('word_length'), \
		f.regexp_extract('value', body_regex, 1).alias('html_body'))\
	.withColumn('id', f.monotonically_increasing_id())\
	.withColumn('body', html_to_plain_udf('html_body'))
	
	send_to_cassandra.select('id', 'sender', 'receiver', 'subject', 'body').write.csv(output, mode='overwrite')
	
	#send_to_cassandra.write.format("org.apache.spark.sql.cassandra")\
	#	.options(table = 'email', keyspace = 'email_database').save()
	
	
	
	
	
	
	
	
	
if __name__ == '__main__':
	inputs = sys.argv[1]
	output = sys.argv[2]
	spark = SparkSession.builder.appName('email ETL')\
		.config('spark.cassandra.connection.host', cassandra_host).getOrCreate()
	spark.sparkContext.setLogLevel('WARN')
	main(inputs, output)
