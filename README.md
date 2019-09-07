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

*The 'Horses for Courses' datasets used in this project were downloaded originally from [data.world](https://data.world/sya/horses-for-courses) and [Tipster Bets](https://data.world/data-society/horse-racing-tipster-bets)*

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

- id
- age
- sex_id - see horse_sexes.csv
- sire_id - not related to horses.id, there is another table called horse_sires that is not present here
- dam_id - not related to horses.id, there is another table called horse_dams that is not present here
- prize_money - total aggregate prize money

***
**odds.csv** - *(collected for every runner 10 minutes out from race start until race starts)*

- runner_id
- collected _what time was this row created/data collected, datetime in UTC_
- odds_one_win
- from odds source, win odds
- odds_one_win_wagered - from odds source, rough $ amount wagered on win
- odds_one_place - from odds source, place odds
- odds_one_place_wagered - from odds source, rough $ amount wagered on place
- odds_two_win
- odds_two_win_wagered
- odds_two_place
- odds_two_place_wagered
- odds_three_win
- odds_three_win_wagered
- odds_three_place
- odds_three_place_wagered
- odds_four_win
- odds_four_win_wagered
- odds_four_place
- odds_four_place_wagered

***
**runners.csv** - *Details about horses' performance in races*

- id
- collected - what time was this row created/data collected, datetime in UTC
- market_id
- position - Will either be 1,2,3,4,5,6 etc or 0/null if the horse was scratched or failed to finish - If all positions for a market_id are null it means we were unable to match up the positional data for this market
- place_paid - Will either be 1/0 or null - If you see a race that only has 2 booleans of 1 it means that the race only paid out places on the first two positions
- margin - If the runner didnt win, how many lengths behind the 1st place was it
- horse_id - see horses.csv
- trainer_id
- rider_id - see riders.csv
- handicap_weight
- number
- barrier
- blinkers
- emergency - did it come into the race at the last minute
- form_rating_one
- form_rating_two
- form_rating_three
- last_five_starts
- favourite_odds_win - from one of the odds sources, will it win - true/false
- favourite_odds_place - from one of the odds sources, will it win - true/false
- favourite_pool_win
- favourite_pool_place
- tip_one_win - from a tipster, will it win - true/false
- tip_one_place - from a tipster, will it place - true/false
- tip_two_win
- tip_two_place
- tip_three_win
- tip_three_place
- tip_four_win
- tip_four_place
- tip_five_win
- tip_five_place
- tip_six_win
- tip_six_place
- tip_seven_win
- tip_seven_place
- tip_eight_win
- tip_eight_place
- tip_nine_win
- tip_nine_place

***
**forms.csv** - *main fact file*

- collected - what time was this row created/data collected, datetime in UTC
- market_id
- horse_id
- runner_number
- last_twenty_starts -e.g. f9x726x753x92222x35 - f = failed to finish, 7 = finished 7th, 6 = finished 6th, 7 = finished 7th, x = runner was scratched
- class_level_id - 1 = eq (in same class as other horses) - 2 = up (up in class) - 3 = dn (down in class)
- field_strength
- days_since_last_run
- runs_since_spell
- overall_starts
- overall_wins
- overall_places
- track_starts
- track_wins
- track_places
- firm_starts
- firm_wins
- firm_places
- good_starts
- good_wins
- good_places
- dead_starts
- dead_wins
- dead_places
- slow_starts
- slow_wins
- slow_places
- soft_starts
- soft_wins
- soft_places
- heavy_starts
- heavy_wins
- heavy_places
- distance_starts
- distance_wins
- distance_places
- class_same_starts
- class_same_wins
- class_same_places
- class_stronger_starts
- class_stronger_wins
- class_stronger_places
- first_up_starts
- first_up_wins
- first_up_places
- second_up_starts
- second_up_wins
- second_up_places
- track_distance_starts
- track_distance_wins
- track_distance_places

***
**conditions.csv** - *Describes running conditions - e.g. good, firm, etc.*

- id
- name

***
**weathers.csv**

- id
- name

***
**riders.csv** - *Describes sex of the jockey*

- id
- sex

***
**horse_sexes.csv**

- id
- name

***

#### JSON File

There is 1 JSON file used as input data - the schema has been copied here from the data.world website page which describes this dataset, and is linked to above.

***
**tips.json** - *Details on punters*

- UID - Unique ID.
- ID - Each tipsters bets are kept in date and time order with the ID being incremented for each new bet, for each tipster.
- Tipster - The name of the tipster. Each tipsters bets are in order, followed by the next tipsters bets
- Date - The date of the race.
- Track - The name of the track.
- Horse - The name of the horse.
- Bet Type - Is the bet a 'Win' bet or an 'Each Way' bet.
- Odds - The odds that the tipster presenting the bet say they got for the bet.
- Result - Did the bet Win or Lose.
- Tipster Active - Is the tipster active - true or false

***

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

