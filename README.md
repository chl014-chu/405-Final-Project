# MSBA 405 Final Project

**Team 5**

## Pipeline to Analyze CitiBike Usage Patterns and NYC Air Quality

This repository contains a "fire and forget" pipeline designed to analyze the relationship between CitiBike usage during morning rush hours and NYC air quality data. 

---

## Datasets

### CitiBike Data
- **Source:** https://s3.amazonaws.com/tripdata/index.html
- **Description:** Initially, we collected CitiBike trip histories spanning multiple years. However, to meet memory constraints, we narrowed our focus to morning rush hour trips from 2017 to 2019. The dataset has been preprocessed (cleaned and trimmed) and is available for download from our Kaggle repository.
- **Download Command:**

```bash
#!/bin/bash
kaggle datasets download chuchu33/citibike-nyc-morning-commute-data-2017-2019
```

### NYC Air Quality Data
- **Source:** https://www.epa.gov/outdoor-air-quality-data/download-daily-data
- **Description:** We obtained PM2.5 data, including the air quality index, for the "New York-Newark-Jersey City, NY-NJ-PA" region from 2017 to 2019. After downloading, we preprocessed the data to remove entries outside of New York City and merged the annual data into one comprehensive dataset.
- **Download Command:**

```bash
#!/bin/bash
kaggle datasets download chuchu33/consolidated-nyc-air-quality-data-2017-2019
```

---

## Setup

Please clone the repository with our code and move the data files into the `data` directory for the pipeline. Also, please ensure that the files are correctly placed and that all dependencies are installed. Thank you!

---

## Pipeline Execution

Navigate to the repository root directory and execute:

```bash
bash pipeline.sh
```

---

## Desired Repository Layout

```
team5/
├── bash/
│   ├── pipeline.sh       # Main pipeline script to execute Spark and DuckDB workflows
├── data/
│   ├── 2017_citibike_morning_rush_07_09.csv
│   ├── 2018_citibike_morning_hours_07_09.csv
│   ├── 2019_citibike_morning_rush_07_09.csv
│   ├── nyc_air_quality_1719.csv
├── duckdb/
│   ├── final.db          # Database storing processed data
│   ├── queries.sql       # SQL analysis
├── spark/
│   ├── spark.py          # Spark script for data transformation
├── output/
├── README.md
```
