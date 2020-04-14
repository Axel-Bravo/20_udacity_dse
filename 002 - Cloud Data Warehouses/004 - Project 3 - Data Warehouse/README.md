# Project 
In this project we are charged by a startup called _Sparkify_ to create a AWS Redshift database from JSON logs locate 
into an S3 Bucket. The project consist on the creation of an ETL pipeline on python that will launch the appropriated
SQL queries to Redshift. 

The database has *__two zones__*:
1.  __Staging__: from the _S3 bucket_ into the _staging tables_.
2. __Golden__: from the _staging tables_ into the _analytics tables_.

## Database Schema
### Staging Zone
Tables composing the staging database' tables are:
- staging events table
  - artist: __varchar__
  - auth: __varchar__
  - first_name: __varchar__
  - gender: __varchar__
  - item_in_session: __integer__
  - last_name: __varchar__
  - length: __float__
  - level: __varchar__
  - location: __varchar__
  - method: __varchar__
  - page: __varchar__
  - registration: __float__
  - session_id: __integer__
  - song: __varchar__
  - status: __integer__
  - ts: __timestamp__
  - user_agent: __varchar__
  - user_id : __integer__
  
 - staging songs table
   - num_songs: __integer__
   - artist_id: __varchar__
   - artist_latitude: __float__
   - artist_longitud: __float__
   - artist_location: __varchar__
   - artist_name: __varchar__
   - song_id: __varchar__
   - title: __varchar__
   - duration: __float__
   - year: __integer__
 

### Golden Zone
Tables composing the golden database' tables are: 
- songplays' table
  - songplay_id: __varchar__
  - start_time: __bigint__
  - user_id: __integer__
  - level: __varchar__
  - song_id: __varchar__
  - artist_id: __varchar__
  - session_id: __integer__
  - location: __varchar__
  - user_agent: __varchar__

- users' table
  - user_id: __integer__
  - first_name: __varchar__
  - last_name: __varchar__
  - gender: __varchar__
  - level: __varchar__
  
- songs' table
    - song_id: __varchar__
    - title: __varchar__
    - artist_id: __varchar__
    - year: __integer__
    - duration: __float__

- artist's table:
    - artist_id: __varchar___
    - name:  __varchar__
    - location: __varchar__
    - latitude: __float__
    - longitude: __float__
    
 - time's table:
    - start_time: __timestamp__
    - hour: __integer__
    - day: __integer__
    - week: __integer__
    - month: __integer__
    - year: __integer__
    - weekday: __integer__

## Files
The project consists on:
 - __create_tables.py__ : python scripts that creates/resets the table structure
 - __etl.py__: python scripts that uploads the different data, found in JSON format into the Redshift database
 - __sql_queries.py__: python script containing all SQL queries employed in the project
 
 
## Execution instructions
 1. Launch a AWS Redshift DB and properly populate the parameters on the configuration file `dwh.cfg`.
 2. Execute *create_tables.py* on the terminal to create the Redshift database and tables: `python create_tables.py`
 3. Execute *etl.py* to batch upload the data into the correspondent tables: `python etl.py`

