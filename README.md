# ETL Pipeline for city bikes of Helsinki 
### Data Engineering Capstone Project

#### Project Summary
The goal is to create a database where data scientists or data analysts can describe or make predictions. The created database can answer e.g. for the following questions:

1. What are the rush hours for bike stations?
2. How to predict, how many bikes are needed in which stations and when?
3. How many bike trips were made?
4. What are the distances and durations on bike trips?
5. How weather conditions affect to bike trips?



In this project, we are gathering data from city bikes of Helsinki and weather. The main idea is to clean and prepare datasets for data scientists and data analysts that they can make innovative descriptions and predictions. First, I made a simple ETL pipeline, that is very straight forward. Later on, it is possible to use and evaluate the same pipeline 
to prepare all historical data and make OLAP versions for predicting ongoing bike business.

Describe and Gather Data
Dataset can be found here: City bike stations’ Origin-Destination (OD) data includes all trips made with city bikes of Helsinki and Espoo. The data includes information about the trip’s origin and destination stations, start and end times, distance (in meters) as well as duration (in seconds). https://hri.fi/data/en_GB/dataset/helsingin-ja-espoon-kaupunkipyorilla-ajatut-matkat Finnish Meteorological Institute Instantaneous weather observations are available from 2010, daily, and monthly observations from the 1960s onwards (depending on weather station). https://en.ilmatieteenlaitos.fi/download-observations
