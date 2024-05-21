import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, conn, filepath):
    
    # opening song file
    df = pd.read_json(filepath, typ='series')
   
    if df.artist_id and df.song_id:
        
        # inserting artist record
        artist_data = (df.artist_id, df.artist_name, df.artist_location, df.artist_latitude, df.artist_longitude)

        try:
            cur.execute(artists_table_insert, artist_data)
        except psycopg2.Error as e:
            return e
    
        # inserting song record
        song_data = (df.song_id, df.title, df.artist_id, df.year, df.duration)
        
        try:
            cur.execute(songs_table_insert, song_data)
        except psycopg2.Error as e:
            return e

def process_log_file(cur, conn, filepath):
    # opening log file
    df = pd.read_json(filepath, lines=True)

    # filtering by NextSong action
    df = df[df['page'] == 'NextSong']

    # converting timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('Timestamp', 'Hour', 'Day', 'Week', 'Month', 'Year', 'Weekday')
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            return e
    # load user table
    
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        if row.userId:
            try:
                cur.execute(users_table_insert, list(row))
            except psycopg2.Error as e:
                return e
                
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            return e
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row['ts'], unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplays_table_insert, songplay_data)
        except psycopg2.Error as e:
            return e

def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        e = func(cur, conn, datafile)
        if e:
            conn.rollback()
            print('Error while processing file {}: {}'.format(i, e))
        else:
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()