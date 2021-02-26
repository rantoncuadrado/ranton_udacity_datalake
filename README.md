
# PURPOSE OF THIS DATABASE

Sparkify is a music streaming startup that has gron their user base and song database. So they want to move their DWH to a data lake. 

# DATABASE SCHEMA DESIGN AND ETL PIPELINE

Sparkify data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## DataSets

### DS1. Song Dataset
A set of files in JSON format containing metadata about songs and their authors/artists stored in a S3 bucket s3://udacity-dend/song_data

e.g. of names are:
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

Example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### DS2. Log Dataset
A set of log files in JSON format from the streaming app.

The log files are partitioned by year and month and day and are in another s3 bucket Log data: s3://udacity-dend/log_data. 

E.g. files:
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json

The data that can be found there:
	artist	auth	firstName	gender	itemInSession	lastName	length	level	location	method	page	registration	sessionId	song	status	ts	userAgent	userId


## Purpose

Build an ETL pipeline for a data lake hosted on S3. 

This means load data from S3, process the data into analytics tables using Spark, and load them back into S3. 


