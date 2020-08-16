# ETL Pipeline for city bikes of Helsinki 
### Data Engineering Capstone Project

#### Project Summary
The goal is to create a database where data scientists or data analysts can describe the data or make predictions. The created database can answer e.g. for the following questions:

1. What are the rush hours for bike stations?
2. How to predict, how many bikes are needed in which stations and when?
3. How many bike trips were made?
4. What are the distances and durations on bike trips?
5. How weather conditions affect to bike trips?


In this project, we are gathering data from city bikes of Helsinki and areal weather of Helsinki. The main idea is to clean and prepare datasets for data scientists and data analysts that they can make descriptions and predictions. First, I made a simple ETL pipeline, that is very straight forward. Later on, it is possible to use and evaluate the same pipeline to prepare all historical data for the describing analysis and evaluate machine learning predictions. Also, it is possible to make OLAP versions for updating the data every hour during the city bikes season predicting ongoing bike business.

I used Spark for processing big files and Pandas for preliminary exploratory data analysis. Data was first in the AWS S3 bucket and then it was cleaned and processed to create dimension and fact tables. After processing, these files were transformed back as parquet files to S3. AWS EMR cluster was needed during this process.

#### Describe and Gather Data
Dataset can be found here: City bike stations’ Origin-Destination (OD) data includes all trips made with city bikes of Helsinki and Espoo. The data includes information about the trip’s origin and destination stations, start and end times, distance (in meters) as well as duration (in seconds). https://hri.fi/data/en_GB/dataset/helsingin-ja-espoon-kaupunkipyorilla-ajatut-matkat Finnish Meteorological Institute Instantaneous weather observations are available from 2010, daily, and monthly observations from the 1960s onwards (depending on weather station). https://en.ilmatieteenlaitos.fi/download-observations

#### Explore the Data
I made a preliminary data exploration and founded out, that these datasets had quite minimal issues with missing or duplicated values. From the quality checks, end of this notebook, we can see how many records were removed. 
The main thing with cleaning the data, was to cast the right data types and setting up Date (timestamp data type) relation with city bikes data and weather data. For the machine learning tables, I made tables for 10 and 60 minutes intervals.

#### Cleaning Steps
Bikes data:
- cast right data types
- rename columns
- create new features: extract year, month, day, weekday and hour from the timestamp 
- drop duplicates
- made id referring every departure
- made counter referring every id
- prepare 10 and 60 min intervals for machine learning tables
- fill NaN values with 0


Weather data:

- added DateTime to weather data files, this was done in pre EDA
- cast right data types
- rename columns
- calculate average weather conditions for 60 min intervals 
- fill NaN values with 0
- drop duplicates

Stations data:
- cast right data types
- rename columns
- fill NaN values with 0


### Define the Data Model
#### Conceptual Data Model
For the main purpose the simplified star schema model suits best. I prepared a fact table to start quick analysis.

Fact table (fact_table) 
id, year, month, day, weekday, hour, departure station name, departure time, return time, return station name, departure station id, return station id, distance, duration, city, longitude, latitude, count

Bikes dimension table (dim_bikes_orig)
id, departure time, return time, departure station id, departure station name, return station id, return station name, distance, duration, year, month, day, weekday, hour

Bikes dimension table for 10 min (dim_bikes_10min)
id, departure time, return time, departure station id, departure station name, return station id, return station name, distance, duration, year, month, day, weekday, hour, date

Bikes dimension table for 60 min (dim_bikes_60min)
id, departure time, return time, departure station id, departure station name, return station id, return station name, distance, duration, year, month, day, weekday, hour, date

Weather dimension table (dim_temp_10min) 
date, time zone, cloud amount, pressure, relative humidity, precipitation intensity, air temperature, dew-point temperature, horizontal visibility, wind direction, gust speed, wind speed

Stations dimension table (dim_stations) 351 records
station id, city, name, address, operator, capacity, longitude, latitude

Then I also prepared two tables for machine learning to make a prediction machine.

ML table for 10 min interval (ml_bikes_10min)
date, year, month, day, weekday, hour, air temperature, humidity, wind speed, bike count

ML table for 60 min interval (ml_bikes_60min)
date, year, month, day, weekday, hour, air temperature, humidity, wind speed, bike count


#### 3.2 Mapping ETL Pipeline

Use an effective way to clean and create tables

1. Bikes data from years 2017-2019 in dataframe to clean and create a dim table 
2. Clean and create dim bikes 10min table
3. Clean and create dim bikes 60min table
4. Make a counter from original data (column count)
5. Make a counter for bike trips in 10 min intervals
6. Make a counter for bike trips in 60 min intervals
7. Weather data in dataframe temp_df to clean and create a dim table in 10 min intervals
8. Clean and create a table in 60 min intervals
9. Calculate average weather conditions for 60 min intervals 
10. Clean and create a stations table 
11. Create tempView from bikes data, stations data and bike count to make a fact table
12. Write a fact table to parquet file as bikes_fact_2017_2019
13. Create tempView from bikes 10min, temp 10min and bike count 10min and make bikes ml table
14. Create tempView from bikes 60min, temp 60min and bike count 60min and make bikes ml table
15. Make data quality checks

