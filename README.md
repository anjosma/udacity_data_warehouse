# Data Warehouse
This is the third project from Nanodegree Data Engineering from Udacity. The project aims to create and modeling an DW (Data Warehouse) in Amazon Redshift environment in a startup called Sparkfy. Also, it uses S3 to storage `json` files

## Data
The data we use is available in two differents folders in S3 bucket : `s3://udacity-dend/log-data` and `s3://udacity-dend/song-data`.

## About the tables
### Fact Table
**songplays** - records in event data associated with song plays i.e. records with page NextSong:
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
**users** - users in the app:
- user_id, first_name, last_name, gender, level  

**songs** - songs in music database:
- song_id, title, artist_id, year, duration  

**artists** - artists in music database:
- artist_id, name, location, latitude, longitude  

**time** - timestamps of records in songplays broken down into specific units:
- start_time, hour, day, week, month, year, weekday  

## How run the project?
### Steps
- `etl.py`
- `create_table.py`

First, clone this GIT repository into your local machine and install the requirements libraries using:
```console
$ pip install -r requirements.txt
```
Now, the environment is ready! 

Before running the scripts, we need to fill the configuration file `dwh.cfg` with database information and `ARN` string from Amazon Web Services.
```ini
[CLUSTER]
HOST=example@example.com
DB_NAME=dev
DB_USER=user
DB_PASSWORD=password
DB_PORT=5439

[IAM_ROLE]
ARN=xxxxxxxxxxxxxxxxx

[S3]
LOG_DATA=s3://udacity-dend/log-data
LOG_JSONPATH=s3://udacity-dend/log_json_path.json
SONG_DATA=s3://udacity-dend/song-data

```
Execute the following script:
```console
$ python3 create_tables.py
```
The command above will create all the tables from S3 structured folders into tables in Amazon Redshift. Then, we need to run the script responsible for the ETL:
```console
$ python3 etl.py
```
This command starts the ETL process and gather data from `s3://udacity-dend/log-data` and `s3://udacity-dend/song-data` to store them in Amazon Redshift tables created previously.
