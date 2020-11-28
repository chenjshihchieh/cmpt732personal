CREATE KEYSPACE email_database WITH REPLICATION = {
	'class': 'SimpleStrategy', 'replication_factor': 1 };
	
USE email_database;

CREATE TABLE email(id bigint PRIMARY KEY, sender text, receiver text, subject text, body text);