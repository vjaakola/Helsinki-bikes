from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *


class Operators:
    
    """
    Clean and create views and tables
    """
    @staticmethod
    def df_clean_create_orig(df, output_data):

        """
        clean and create original bike trips dim table, adding id for getting bike count later on 
        """
        df = df\
            .withColumn('id', monotonically_increasing_id())\
            .withColumn('departure', col('Departure').cast('timestamp'))\
            .withColumn('return', col('Return').cast('timestamp'))\
            .withColumn('dep_station_id', col('Departure station id').cast('integer'))\
            .withColumnRenamed('Departure station name', 'dep_station_name') \
            .withColumn('ret_station_id', col('Return station id').cast('integer'))\
            .withColumnRenamed('Return station name', 'ret_station_name') \
            .withColumn('distance', col('Covered distance (m)').cast('integer'))\
            .withColumnRenamed('Duration (sec.)', 'duration')\
            .withColumn('year', year('departure'))\
            .withColumn('month', month('departure'))\
            .withColumn('day', dayofmonth('departure'))\
            .withColumn('weekday', date_format(col('departure'), 'EEEE'))\
            .withColumn('hour', hour('departure'))\
            .dropDuplicates()\
            .drop('Departure station id', 'Departure station name','Return station id','Return station name','Covered distance (m)','Duration (sec.)')\
            .fillna(0)

        # write dimension to parquet file
        output_data = 'helsinki_bikes/results/'
        df.write.parquet(output_data + "dim_bikes_orig", mode="overwrite")
        return df


    @staticmethod
    def df_clean_create_10min(df, output_data):

        """
        clean and create bike trips dim table for 10 min interval
        preparingin option to join with 10 min temp table
        """
        df = df\
            .withColumn('departure', col('Departure').cast('timestamp'))\
            .withColumn('return', col('Return').cast('timestamp'))\
            .withColumn('dep_station_id', col('Departure station id').cast('integer'))\
            .withColumnRenamed('Departure station name', 'dep_station_name') \
            .withColumn('ret_station_id', col('Return station id').cast('integer'))\
            .withColumnRenamed('Return station name', 'ret_station_name') \
            .withColumn('distance', col('Covered distance (m)').cast('integer'))\
            .withColumnRenamed('Duration (sec.)', 'duration')\
            .withColumn('date', ((round(unix_timestamp(col("departure")) / 600) * 600).cast('timestamp'))) \
            .withColumn('year', year('date'))\
            .withColumn('month', month('date'))\
            .withColumn('day', dayofmonth('date'))\
            .withColumn('weekday', date_format(col('date'), 'EEEE'))\
            .withColumn('hour', hour('date'))\
            .drop('Departure station id', 'Departure station name','Return station id','Return station name','Covered distance (m)','Duration (sec.)')\
            .fillna(0)\
        
        return df



    @staticmethod
    def df_clean_create_60min(df, output_data):

        """
        clean and create bike trips dim table for 60 min interval
        preparingin option to join with 60 min temp table
        """
        df = df\
            .withColumn('departure', col('Departure').cast('timestamp'))\
            .withColumn('return', col('Return').cast('timestamp'))\
            .withColumn('dep_station_id', col('Departure station id').cast('integer'))\
            .withColumnRenamed('Departure station name', 'dep_station_name') \
            .withColumn('ret_station_id', col('Return station id').cast('integer'))\
            .withColumnRenamed('Return station name', 'ret_station_name') \
            .withColumn('distance', col('Covered distance (m)').cast('integer'))\
            .withColumnRenamed('Duration (sec.)', 'duration')\
            .withColumn('date', ((round(unix_timestamp(col("departure")) / 3600) * 3600).cast('timestamp'))) \
            .withColumn('year', year('date'))\
            .withColumn('month', month('date'))\
            .withColumn('day', dayofmonth('date'))\
            .withColumn('weekday', date_format(col('date'), 'EEEE'))\
            .withColumn('hour', hour('date'))\
            .drop('Departure station id', 'Departure station name','Return station id','Return station name','Covered distance (m)','Duration (sec.)')\
            .fillna(0)\
        
        return df
        

        """
        clean and create temp dim table for 10 min interval
        """

    @staticmethod
    def temp_clean_create_10min(temp_df, output_data):
        temp_df = temp_df\
            .withColumn('date', col('Date').cast('timestamp'))\
            .withColumn('cloud_amount', col('Cloud amount (1/8)').cast('float'))\
            .withColumn('pressure', col('Pressure (msl) (hPa)').cast('float'))\
            .withColumn('humidity', col('Relative humidity (%)').cast('float'))\
            .withColumn('precipitation', col('Precipitation intensity (mm/h)').cast('float'))\
            .withColumn('air_temp', col('Air temperature (degC)').cast('float'))\
            .withColumn('dew_point_temp', col('Dew-point temperature (degC)').cast('float'))\
            .withColumn('visibility', col('Horizontal visibility (m)').cast('float'))\
            .withColumn('wind_direc', col('Wind direction (deg)').cast('float'))\
            .withColumn('gust_speed', col('Gust speed (m/s)').cast('float'))\
            .withColumn('wind_speed', col('Wind speed (m/s)').cast('float'))\
            .withColumnRenamed('Time zone', 'time_zone')\
            .dropDuplicates()\
            .drop('Cloud amount (1/8)','Pressure (msl) (hPa)','Relative humidity (%)','Precipitation intensity (mm/h)','Air temperature (degC)', 'Dew-point temperature (degC)','Horizontal visibility (m)','Wind direction (deg)','Gust speed (m/s)','Wind speed (m/s)','Time zone' )\
            .fillna(0)

        # write dimension to parquet file
        output_data = 'helsinki_bikes/results/'
        temp_df.write.parquet(output_data + "dim_temp_10min", mode="overwrite")
        return temp_df



        """
        clean and create temp dim table for 60 min interval
        count average weather conditions for 60 min in notebook
        """

    @staticmethod
    def temp_clean_create_60min(temp_df, output_data):
        temp_df = temp_df.withColumn('date', ((round(unix_timestamp(col("Date")) / 3600) * 3600).cast('timestamp')))

        return temp_df


        """
        clean and create stations dim table
        """

    @staticmethod
    def stations_clean_create(stations_df, output_data):
        stations_df = stations_df\
        .withColumn('station_id', col('ID').cast('integer'))\
        .withColumn('name', col('Nimi').cast('string'))\
        .withColumnRenamed('Osoite', 'address')\
        .withColumnRenamed('Kaupunki', 'city')\
        .withColumnRenamed('Operaattor', 'operator')\
        .withColumn('capacity', col('Kapasiteet').cast('integer'))\
        .withColumn('longitude', col('x').cast('float'))\
        .withColumn('latitude', col('y').cast('float'))\
        .fillna(0)\

        # write dimension to parquet file
        output_data = 'helsinki_bikes/results/'
        stations_df.write.parquet(output_data + "dim_stations", mode="overwrite")
        return stations_df
