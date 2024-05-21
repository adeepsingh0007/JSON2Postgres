# DROP TABLES

songplays_table_drop = "DROP TABLE IF EXISTS songplays"
users_table_drop = "DROP TABLE IF EXISTS users"
songs_table_drop = "DROP TABLE IF EXISTS songs"
artists_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

songplays_table_create = ("""
CREATE TABLE songplays(
songplay_id serial PRIMARY KEY,
start_time timestamp,
user_id int REFERENCES users(user_id),
level varchar,
song_id varchar REFERENCES songs(song_id),
artist_id varchar REFERENCES artists(artist_id),
session_id int,
location varchar,
user_agent varchar
)
""")

users_table_create = ("""
CREATE TABLE users(
user_id int PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
""")

songs_table_create = ("""
CREATE TABLE songs(
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar REFERENCES artists(artist_id),
year int,
duration numeric
)
""")

artists_table_create = ("""
CREATE TABLE artists(
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude numeric,
longitude numeric
)
""")

time_table_create = ("""
CREATE TABLE time_table(
start_time timestamp,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
""")

# INSERT RECORDS

songplays_table_insert = ("""
INSERT INTO songplays VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s)
""")

users_table_insert = ("""
INSERT INTO users VALUES (%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO NOTHING
""")

songs_table_insert = ("""
INSERT INTO songs VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING
""")

artists_table_insert = ("""
INSERT INTO artists VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time_table VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""SELECT s.song_id songid, a.artist_id artistid
FROM songs s
JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title = %s AND a.name = %s AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [artists_table_create, songs_table_create, users_table_create, songplays_table_create, time_table_create]
drop_table_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]