#!/bin/bash

DUCKDB=duckdb
SPARK=spark-submit

TRANSFORMER=../spark/spark.py

DATABASE=../duckdb/final.db
OUTPUT=../output
QUERIES=../duckdb/queries.sql

# Data input paths.
CITIBIKE="../data/2017_citibike_morning_rush_07_09.csv,../data/2018_citibike_morning_hours_07_09.csv,../data/2019_citibike_morning_rush_07_09.csv"
AIR_QUALITY=../data/nyc_air_quality_1719.csv

# Variable for DuckDB load.
LOADPATH="$OUTPUT/*.parquet"

# Rollback function to clean up any output if a step fails.
rollback() {
    rm -fr $OUTPUT
    rm -f $DATABASE
}

# Function for printing messages.
message() {
    printf "%50s\n" | tr " " "-"
    printf "$1\n"
    printf "%50s\n" | tr " " "-"
}

# Check exit status; if a command fails, rollback and exit.
check() {
    if [ $? -eq 0 ]; then
        message "$1"
    else 
        message "$2"
        rollback
        exit 1
    fi
}

# Run the Spark job.
run_spark() {
    rm -fr $OUTPUT
    $SPARK \
        --master local[*] \
        --conf "spark.sql.shuffle.partitions=2" \
        --name "Citibike and Air Quality Analysis" \
        $TRANSFORMER \
            "$CITIBIKE" \
            "$AIR_QUALITY" \
            $OUTPUT
    check "Spark job successfully completed." "Spark job FAILED."
}

# Load the Parquet output into DuckDB.
run_duckdb() {
    sed "s|\$LOADPATH|${LOADPATH//\//\\/}|g" "$QUERIES" | $DUCKDB "$DATABASE"
    check "Data loaded into DuckDB successfully." "Data load FAILED."
}

message "\n\n\n\nSTARTING FINAL PIPELINE...\n\n\n\n"

run_spark
run_duckdb

check "PROCESS COMPLETE" "PIPELINE FAILED"

