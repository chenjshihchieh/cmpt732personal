import sys
from pyspark.sql import SparkSession, functions as f, types

sender_regex = r'From:[^<]+<([^\n]+)>'
receiver_regex = r'To: ([^\n]+)'
body_regex = r'\n\n([\H\n\s]*)'



def main(inputs, output):
	
	raw_email = spark.read.text(intputs, wholetext = True)
	from_to = raw_email.select(\
		f.regexp_extract('value', sender_regex, 1).alias('sender'), \
		f.regexp_extract('value', receiver_regex, 1).alias('receiver'), \
		f.regexp_extract('value', body_regex, 1).alias('body'))
	from_to.write.csv(output, mode = 'overwrite')
	
if __name__ == '__main__':
	inputs = sys.argv[0]
	output = sys.argv[1]
    spark = SparkSession.builder.appName('email ETL').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    main(inputs, output)