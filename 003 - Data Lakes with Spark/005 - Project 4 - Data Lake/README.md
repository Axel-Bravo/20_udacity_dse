# Project  # TODO
In this project we are charged by a startup called _Sparkify_ to create a Big data pipeline based on an architecture of:
 - *Storage*: on S3 containers (both for staging and Golden Tables)
 - Processing: executed on Spark
 
The project consist on the creation of an ETL pipeline on python that will launch the appropriated python
transformations to AWS infrastructure


## Tables Schema
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
    - artist_id: __varchar__
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
 - __etl.py__: python scripts that download - transform and upload the different data, found in JSON format into another
               S3 bucket (once processed)
 - __etl.ipynd__: Jupyter Notebook employed for fast development and testing
 
 
## Execution instructions
 1. Launch a AWS EMR with Spark and properly populate the parameters on the configuration file `dwh.cfg`.
 2. Execute *etl.py* to batch download - transform - upload the data into the correspondent files: `python etl.py`