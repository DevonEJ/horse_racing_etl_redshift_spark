# ETL Pipeline; Pony Punts' Weekly Betting Insights Pipeline

*Udacity Data Engineering Nanodegree capstone project.*

## Project Overview

Pony Punts is a fictional horse racing betting statistics company, which combines data sourced via numerous web-based sources, which are then analysed by the in-house Analytics Team, to provide weekly betting insights and tips for its customers.

### Project Purpose

The purpose of the ETL pipeline, is to run once on a weekly basis, to transform datasets curated and stored in Amazon S3, transforming, and eventually loading the data as a set of 3 analytics tables used by the Pony Punts Analytics Team to run their analysis queries in SQL.

The pipeline is written primarily in Python, and also uses PySpark for transformations, and SQL to interact with Redshift via <code> pyodbc </code> . The <code> boto3 </code> Python package is used to interact with Amazon S3.

The high-level pipeline steps are as follows;

1. A set of 9 CSV files are stored in Amazon S3, along with one JSON file
2. A Redshift cluster exists, and a SQL script creates a set of 9 staging tables within
3. SQL scripts execute COPY commands to load each of the 9 CSV files into their respective Redshift staging tables
4. A Python script reads the JSON file from S3
5. PySpark is used to convert each of staging tables into a PySpark dataframe
6. Transformation is carried out in PySpark to clean and join the dataframes, combining to create 3 final analytics dataframes; <code> customers </code> , <code> race_horses </code> , and <code> races </code>
7. The final analytics dataframes are written to S3 as CSV files
8. A SQL script creates the analytics tables in Redshift
9. SQL scripts execute COPY commands to load the final CSV files into the analytics tables
10.The analytics tables are now ready for use by the Pony Punts Analytics Team

Validation and data quality checks are built-in to the pipeline, checking, for example that row counts for staging and analytics tables are as expected at each stage of the pipeline.

Logging has also been implemented to allow quick and easy resolution of any pipeline failures.


### Input Data Schemas

*The 'Horses for Courses' datasets used in this project were downloaded originally from [data.world](https://data.world/sya/horses-for-courses) Also Tipster Bets - https://data.world/data-society/horse-racing-tipster-bets*

#### CSV Files

There are 9 CSV files used as input data - their schemas have been copied here from the data.world website page which describes this dataset, and is linked to above.

These CSV files represent a normalised dataset on horse racing data. The **forms.csv** is the 'face table', whilst the other files represent the dimensions of the dataset.

---

**markets.csv** - *Details about specific races - 3316 rows*
- id
- start_time - what time did the race start, datetime in UTC
- venue_id
- race_number
- distance(m)
- condition_id - track condition, see conditions.csv
- weather_id -  weather on day, see weathers.csv
- total_pool_win_one - rough $ amount wagered across all runners for win market
- total_pool_place_one - rough $ amount wagered across all runners for place market
- total_pool_win_two
- total_pool_place_two
- total_pool_win_three
- total_pool_place_three

***

**horses.csv** - *Details about race horses*




## ETL Pipeline

TBC
 - Inc. data quality checks
 - Inc. the points about future expansion
 
### Output Data Model
 Why did you choose the model you chose?
 
 Using two databases - one for storing the staging tables, and one for the cleaned analytics tables?
 
### Pipeline Steps 

Step 2: Explore and Assess the Data
Explore the data to identify data quality issues, like missing values, duplicate data, etc.
Document steps necessary to clean the data
Step 3: Define the Data Model
Map out the conceptual data model and explain why you chose that model
List the steps necessary to pipeline the data into the chosen data model
Step 4: Run ETL to Model the Data
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/count checks to ensure completeness

## Example Analysis

