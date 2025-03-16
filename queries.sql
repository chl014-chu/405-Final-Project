CREATE TABLE IF NOT EXISTS citibike_rides AS
    SELECT * FROM read_parquet('$LOADPATH');


CREATE OR REPLACE VIEW measures_by_station AS (
    SELECT
        start_station_id,
        ANY_VALUE(start_station_latitude) AS start_station_latitude,
        ANY_VALUE(start_station_longitude) AS start_station_longitude,
        ANY_VALUE("Local Site Name") AS local_site_name,
        ANY_VALUE(site_latitude) AS site_latitude,
        ANY_VALUE(site_longitude) AS site_longitude
    FROM citibike_pollution
    GROUP BY start_station_id
);


CREATE OR REPLACE VIEW measure_by_county AS (
    SELECT
        CAST(trip_start_timestamp AS DATE) AS trip_start_date,
        County,
        COUNT(*) AS total_trips,
        AVG(trip_duration) AS avg_trip_duration
    FROM citibike_rides
    GROUP BY trip_start_date, County
    ORDER BY trip_start_date, County
);


CREATE OR REPLACE VIEW pollution_vs_bike_usage AS (
    SELECT
        EXTRACT(year FROM trip_start_timestamp) AS year,
        EXTRACT(month FROM trip_start_timestamp) AS month,
        "Local Site Name" AS zone,
        COUNT(*) AS total_trips,
        AVG(avg_pm25) AS annual_avg_pm25,
        AVG(avg_aqi) AS annual_avg_aqi
    FROM citibike_rides
    GROUP BY year, month, zone
    ORDER BY year, month, total_trips DESC
);

CREATE OR REPLACE TABLE aqi_trip_duration AS
SELECT
    trip_duration,
    avg_aqi,
    date,
    County,
    CASE
        WHEN avg_aqi = 0 THEN 'Good'
        WHEN avg_aqi BETWEEN 1 AND 50 THEN 'Good'
        WHEN avg_aqi BETWEEN 51 AND 100 THEN 'Moderate'
        WHEN avg_aqi BETWEEN 101 AND 150 THEN 'Unhealthy for Sensitive Groups'
        WHEN avg_aqi BETWEEN 151 AND 200 THEN 'Unhealthy'
        WHEN avg_aqi BETWEEN 201 AND 300 THEN 'Very Unhealthy'
        WHEN avg_aqi > 300 THEN 'Hazardous'
        ELSE 'Unknown'
    END AS aqi_level
FROM citibike_rides;

