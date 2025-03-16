import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, to_timestamp, date_trunc, row_number, split,
    regexp_extract, regexp_replace, when,
    monotonically_increasing_id, broadcast, date_format, to_date, trim,
    avg, first, expr
)
from pyspark.sql import Window

def main():
    """
    Joins Citibike rides data with aggregated Air Quality data on matching date and zone.
    
    Usage:
        spark-submit spark.py \
            [citibike_paths (comma-separated)] \
            [air_quality_path] \
            [output_path]
    """
    # Parse command line arguments; use defaults if incomplete.
    if len(sys.argv) == 4:
        citibike_paths = sys.argv[1].split(",")
        air_quality_path = sys.argv[2]
        output_path = sys.argv[3]
    else:
        print("WARNING: No (or incomplete) arguments were provided.")
        print("Using default file paths. Adjust them as needed.\n")
        citibike_paths = [
            "2017_citibike_morning_rush_07_09.csv",
            "2018_citibike_morning_hours_07_09.csv",
            "2019_citibike_morning_rush_07_09.csv"
        ]
        air_quality_path = "Air_Quality.csv"
        output_path = "joined_output"

    # Create a Spark session.
    spark = SparkSession.builder.appName("CitibikeAirQualityJoin").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    ######################################################
    # Load and preprocess Citibike rides data
    ######################################################

    rides = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(citibike_paths)
        .select(
            col("tripduration").alias("trip_duration"),
            col("starttime").alias("trip_start_time"),
            col("startstationid").alias("start_station_id"),
            col("startstationlatitude").cast("double").alias("start_station_latitude"),
            col("startstationlongitude").cast("double").alias("start_station_longitude"),
            col("endstationid").alias("end_station_id"),
            col("usertype").alias("user_type")
        )
        # Parse timestamp to capture milliseconds.
        .withColumn("trip_start_timestamp", to_timestamp(trim(col("trip_start_time")), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("trip_start_date", date_format(col("trip_start_timestamp"), "yyyy-MM-dd"))
    )

    # Assign a unique identifier to each ride.
    rides = rides.withColumn("ride_id", monotonically_increasing_id())

    ######################################################
    # Load and preprocess Air Quality data.
    ######################################################
    # Read the air quality dataset including lab details.
    air_quality = (
        spark.read
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .csv(air_quality_path)
        .select(
            col("Date"),
            col("`Daily Mean PM2.5 Concentration`").cast("double").alias("pm25_concentration"),
            col("`Daily AQI Value`").alias("air_quality_index"),
            col("State"),
            col("County"),
            col("`Site Latitude`").cast("double").alias("site_latitude"),
            col("`Site Longitude`").cast("double").alias("site_longitude"),
            col("`Local Site Name`").alias("Local Site Name") # Selecting the column to make it available for groupBy
        )
        .withColumn("date", to_date(col("Date"), "MM/dd/yyyy"))
    )

    # Aggregate air quality values per lab (location) and date.
    aq_agg = air_quality.groupBy("Local Site Name", "County", "date", "site_latitude", "site_longitude") \
        .agg(
            avg("pm25_concentration").alias("avg_pm25"),
            avg("air_quality_index").alias("avg_aqi")
        )

    ######################################################
    # Join rides with air quality data based on date.
    ######################################################
    join_df = rides.join(aq_agg, rides.trip_start_date == aq_agg.date, "inner")

    # Compute the squared Euclidean distance between the ride's start station and the lab location.
    join_df = join_df.withColumn(
        "squared_distance",
        pow(col("start_station_latitude") - col("site_latitude"), 2) +
        pow(col("start_station_longitude") - col("site_longitude"), 2)
    )

    ######################################################
    # For each ride, pick the closest lab record.
    ######################################################
    window_spec = Window.partitionBy("ride_id").orderBy(col("squared_distance").asc())
    closest_df = join_df.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("trip_start_time", "trip_start_date", "rn")
    

    # Debug output
    closest_df.show(20, truncate=False)

    lab_counts = closest_df.groupBy("Local Site Name").agg({"ride_id": "count"})
    lab_counts.show(10, truncate=False)
    

    # Write the final dataframe
    closest_df.write \
        .option("maxRecordsPerFile", 100000) \
        .mode("overwrite") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()
