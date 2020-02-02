# Project 
This project consist on an introduction to the workings of a batch loading of files, in JSON format, to a SQL database. The project consists on:
 - __create_tables.py__ : python scripts that creates/resets the table structure
 - __etl.py__: python scripts that uploads the differents data, found in JSON format into the PostGre SQL database
   - elt.ipynb: contains the same code as **etl.py** but developed step by step, easening understanding and/or debugging.
 - __test.ipynb__ : JB helful to visualize the different PostGRE tables, so as to ensure proper data upload
 
 
## Execution instructions
 1. Execute *create_tables.py* on the terminal to create the PostGRE database and tables
 2. Execute *etl.py* to batch upload the data on data folder into the correspondent tables
 3. VIsualize the proper table population with the *test.ipynb*
