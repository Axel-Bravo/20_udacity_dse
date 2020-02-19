# Project 
In this project we are charged by a startup called _Sparkify_ to create a Postgre SQL database with tables designed to optimize the querries on played songs. This is concretized by creating several database schema and ETL pipeline.

## Database Schema
Tables composing the database are: 
- songplays' table
  - songplay_id: __varchar__
  - start_time: __bigint__
  - user_id: __int__
  - level: __varchar__
  - song_id: __varchar__
  - artist_id: __varchar__
  - session_id: __int__
  - location: __varchar__
  - user_agent: __varchar__

- users' table
  - user_id: __int__
  - first_name: __varchar__
  - last_name: __varchar__
  - gender: __varchar__
  - level: __varchar__
  
- songs' table
    - song_id: __varchar__
    - title: __varchar__
    - artist_id: __varchar__
    - year: __int__
    - duration: __float__

- artist's table:
    - artist_id: __varchar___
    - name:  __varchar__
    - location: __varchar__
    - latitude: __float__
    - longitude: __float__
    
 - time's table:
    - start_time: __timestamp__
    - hour: __int__
    - day: __int__
    - week: __int__
    - month: __int__
    - year: __int__
    - weekday: __int__

## Files
The project consists on:
 - __create_tables.py__ : python scripts that creates/resets the table structure
 - __etl.py__: python scripts that uploads the differents data, found in JSON format into the PostGre SQL database
   - elt.ipynb: contains the same code as __etl.py__ but developed step by step, easening understanding and/or debugging.
 - __test.ipynb__ : JB helful to visualize the different PostGRE tables, so as to ensure proper data upload
 
 
## Execution instructions
 1. Execute *create_tables.py* on the terminal to create the Postgre SQL database and tables: `python create_tables.py`
 2. Execute *etl.py* to batch upload the data on data folder into the correspondent tables: `python etl.py`
 3. Visualize the proper table population with the *test.ipynb*
