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
4. A Python script reads the JSON file into memory as a string
5. PySpark is used to convert each of the files into a PySpark dataframe
6. Transformation is carried out in PySpark to clean and join the dataframes, combining to create 3 final analytics dataframes
7. The final analytics dataframes are written to S3 as CSV files
8. A SQL script creates the analytics tables in Redshift
9. SQL scripts execute COPY commands to load the final CSV files into the analytics tables
10.The analytics tables are now ready for use by the Pony Punts Analytics Team


### Input Data Dictionaries

-- Something on each file, including file size and row count

The 'Horses for Courses' datasets used in this project were downloaded originally from [data.world](https://data.world/sya/horses-for-courses)

Also Tipster Bets - https://data.world/data-society/horse-racing-tipster-bets


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

