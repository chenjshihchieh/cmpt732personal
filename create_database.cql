CREATE KEYSPACE email_database WITH REPLICATION = {
	'class': 'SimpleStrategy', 'replication_factor': 1 };
	
USE email_database;

CREATE TABLE emails (sender TEXT, id UUID, datetime TEXT, receiver TEXT, subject TEXT, body TEXT, label TEXT, PRIMARY KEY (sender, id));