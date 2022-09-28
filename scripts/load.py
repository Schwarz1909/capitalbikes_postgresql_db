#Load the data into the Postgres DB

#Import Libraries
import psycopg2
import pandas as pd
import numpy as np
#import SQLAlchemy
import os
import ping3
from ping3 import verbose_ping

#test if extract finished and continue
ping3.EXCEPTIONS = True
try:
    verbose_ping("extract", interval=5, count=0) 
except:
    pass

#Folder Paths
log_path = '/home/logs/'
csv_path = '/home/csv/'

#read the content of the csv-path to a list 
csv_files = os.listdir(csv_path)

#Create the imported_csv_log
if not os.path.exists(log_path + 'imported_csv_files_log.txt'): 
  imported_csv_files_log = []
else: 
    with open(log_path + "imported_csv_files_log.txt", 'r') as f:
      imported_csv_files_log = f.read().splitlines()
      
for csv in csv_files:
    if csv not in imported_csv_files_log:
        try:
            conn = psycopg2.connect("dbname='postgres' user='postgres' host='postgres' password='postgres' port='5432'") 
            conn.autocommit = True
            cur = conn.cursor()
            
            col = pd.read_csv(csv_path + csv)
            col_tuple = tuple(col.columns.tolist())  

            p = '"'     
            print("creating tmp table")
            #Create tmp Table and copy the csv to it
            sql = """DROP TABLE IF EXISTS tmp;
            CREATE TABLE tmp ({columns});
            COPY tmp ({columns2}) from '{csv_file}'
            DELIMITER ','
            CSV HEADER;""".format(columns=', '.join(str(p + x + p + ' varchar') for x in col_tuple), columns2=', '.join(str(p + x + p) for x in col_tuple), csv_file=csv_path + csv)
     
            cur.execute(sql)
            print('done')
            
            print('getting the column names')
            #Get the column_names of the imported csv file
            sql2 = """SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'tmp';"""
        
            cur.execute(sql2)
            print('done')
            
            print('renaming, changing and dropping columns')
            #Rename columns, change Data types and Drop not used columns
            record = cur.fetchall()
            if 'ride_id' in record[0]:
                sql3 = """ALTER TABLE tmp ALTER COLUMN start_station_id TYPE INTEGER USING start_station_id::INTEGER;
                ALTER TABLE tmp ALTER COLUMN end_station_id TYPE INTEGER USING end_station_id::INTEGER;
                ALTER TABLE tmp ALTER COLUMN start_lat TYPE DOUBLE PRECISION USING start_lat::DOUBLE PRECISION;
                ALTER TABLE tmp ALTER COLUMN start_lng TYPE DOUBLE PRECISION USING start_lng::DOUBLE PRECISION;
                ALTER TABLE tmp ALTER COLUMN end_lat TYPE DOUBLE PRECISION USING end_lat::DOUBLE PRECISION;
                ALTER TABLE tmp ALTER COLUMN end_lng TYPE DOUBLE PRECISION USING end_lng::DOUBLE PRECISION;
                ALTER TABLE tmp ALTER COLUMN started_at TYPE TIMESTAMP WITHOUT TIME ZONE USING started_at::TIMESTAMP WITHOUT TIME ZONE;
                ALTER TABLE tmp ALTER COLUMN ended_at TYPE TIMESTAMP WITHOUT TIME ZONE USING ended_at::TIMESTAMP WITHOUT TIME ZONE;
                """
            if 'Duration' in record[0]:
                sql3="""ALTER TABLE tmp RENAME COLUMN "Start date" TO started_at;
                ALTER TABLE tmp RENAME COLUMN "End date" TO ended_at;
                ALTER TABLE tmp RENAME COLUMN "Start station number" TO start_station_id;
                ALTER TABLE tmp RENAME COLUMN "Start station" TO start_station_name;
                ALTER TABLE tmp RENAME COLUMN "End station number" TO end_station_id;
                ALTER TABLE tmp RENAME COLUMN "End station" TO end_station_name;
                ALTER TABLE tmp RENAME COLUMN "Bike number" TO ride_id;
                ALTER TABLE tmp RENAME COLUMN "Member type" TO member_casual;
                ALTER TABLE tmp ALTER COLUMN start_station_id TYPE INTEGER USING start_station_id::INTEGER;
                ALTER TABLE tmp ALTER COLUMN end_station_id TYPE INTEGER USING end_station_id::INTEGER;
                ALTER TABLE tmp ALTER COLUMN started_at TYPE TIMESTAMP WITHOUT TIME ZONE USING started_at::TIMESTAMP WITHOUT TIME ZONE;
                ALTER TABLE tmp ALTER COLUMN ended_at TYPE TIMESTAMP WITHOUT TIME ZONE USING ended_at::TIMESTAMP WITHOUT TIME ZONE;
                ALTER TABLE tmp ADD COLUMN IF NOT EXISTS rideable_type varchar;
                ALTER TABLE tmp ADD COLUMN IF NOT EXISTS start_lat DOUBLE PRECISION;
                ALTER TABLE tmp ADD COLUMN IF NOT EXISTS start_lng DOUBLE PRECISION;
                ALTER TABLE tmp ADD COLUMN IF NOT EXISTS end_lat DOUBLE PRECISION;
                ALTER TABLE tmp ADD COLUMN IF NOT EXISTS end_lng DOUBLE PRECISION;
                ALTER TABLE tmp DROP COLUMN IF EXISTS "Duration";
                """
            
            cur.execute(sql3)
            print('done')
        
            print('inserting dim_ride table')
            #Insert into dim_ride from the tmp_table 
            sql4 = """INSERT INTO dim_ride (ride_id, rideable_type)
            SELECT DISTINCT ON (ride_id) ride_id, rideable_type FROM tmp WHERE not ride_id is null
            ON CONFLICT (ride_id) DO UPDATE SET rideable_type = COALESCE(excluded.rideable_type, dim_ride.rideable_type);"""
            
            cur.execute(sql4)
            print('done')
            
            print('inserting dim_station table')
            #Insert into dim_station the start_station from the tmp_table
            sql5 = """INSERT INTO dim_station (station_id, station_name, lat, lng) 
            SELECT DISTINCT ON (start_station_id) start_station_id, start_station_name, start_lat, start_lng FROM tmp WHERE NOT start_station_id is null 
            ON CONFLICT (station_id) DO UPDATE SET 
            station_name = COALESCE(excluded.station_name, dim_station.station_name), 
            lat = COALESCE(excluded.lat, dim_station.lat), 
            lng = COALESCE(excluded.lng, dim_station.lng);"""
            
            cur.execute(sql5)
            print('done')
            
            print('inserting dim_station table 2')
            #Insert into dim_station the end_station from the tmp_table
            sql6 = """INSERT INTO dim_station (station_id, station_name, lat, lng) 
            SELECT DISTINCT ON (end_station_id) end_station_id, end_station_name, end_lat, end_lng FROM tmp WHERE not end_station_id is null
            ON CONFLICT (station_id) DO UPDATE SET 
            station_name = COALESCE(excluded.station_name, dim_station.station_name), 
            lat = COALESCE(excluded.lat, dim_station.lat), 
            lng = COALESCE(excluded.lng, dim_station.lng);"""
            
            cur.execute(sql6)
            print('done')
            
            print('inserting fact_rental table')
            #INSERT into fact_rental from the tmp_table
            sql7 = """INSERT INTO fact_rental (ride_id, started_at, ended_at, start_station_id, end_station_id, member_casual)
            SELECT ride_id, started_at, ended_at, start_station_id, end_station_id, member_casual FROM tmp 
            WHERE not (ride_id is null OR start_station_id is null OR end_station_id is null);
            """
            
            cur.execute(sql7)
            print('done')
            
            print('dropping tmp table')
            sql8 = """DROP TABLE IF EXISTS tmp;"""
            
            cur.execute(sql8)
            print('done')
            
            cur.close()
            conn.close()
            
        except:
            print('exception')
            conn.close()      
        
        imported_csv_files_log.append(csv)
        
#Write the imported_csv_log file
with open(log_path + "imported_csv_files_log.txt", 'w') as f:
    for i in imported_csv_files_log:
        f.write("%s\n" % i)