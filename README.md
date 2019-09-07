# ETL Pipeline; Pony Punts' Weekly Betting Insights Pipeline

*Udacity Data Engineering Nanodegree capstone project.*

## Project Overview

Pony Punts is a fictional horse racing betting statistics company, which combines data sourced via numerous web-based sources, which are then analysed by the in-house Analytics Team, to provide weekly betting insights and tips for its customers.

### Project Purpose

The purpose of the ETL pipeline, is to run once on a weekly basis, to transform datasets curated and stored in Amazon S3, transforming, and eventually loading the data as a set of 3 analytics tables used by the Pony Punts Analytics Team to run their analysis queries in SQL.


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
**horses.csv** - *Details about race horses - 14113 rows*

- id
- age
- sex_id - see horse_sexes.csv
- sire_id - not related to horses.id, there is another table called horse_sires that is not present here
- dam_id - not related to horses.id, there is another table called horse_dams that is not present here
- prize_money - total aggregate prize money

***
**odds.csv** - *(collected for every runner 10 minutes out from race start until race starts) - 410023 rows*

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
**runners.csv** - *Details about horses' performance in races - 44428 rows*

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
**forms.csv** - *main fact file - 43293 rows*

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
**conditions.csv** - *Describes running conditions - e.g. good, firm, etc. - 12 rows*

- id
- name

***
**weathers.csv**- *4 rows*

- id
- name

***
**riders.csv** - *Describes sex of the jockey - 1025 rows*

- id
- sex

***
**horse_sexes.csv** - *6 rows*

- id
- name

***

#### JSON File

There is 1 JSON file used as input data - the schema has been copied here from the data.world website page which describes this dataset, and is linked to above.

***
**tips.json** - *Details on punters - 38249 records*

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

The pipeline is written primarily in Python, and also uses PySpark for transformations, and SQL to interact with Redshift via <code> pyodbc </code> . The <code> boto3 </code> Python package is used to interact with Amazon S3.

The cloud-based architecture was chosen to allow for easy integration with the pipelnie and multiple other services, for example, in the case of future expansion of the business, other AWS services can easily be added to allow for scaling.

Both of the existing choices, in Redshift and S3, can also handle far greater volumes than currently running through the pipeline, if required.

In addition, Spark has been used to handle all of the data transformations, due to its speed when compared to purely Pandas-based processing on multiple larger datasets, and again to allow to allow for future scaling easily with no code changes being required.

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
 
### Output Data Schemas

The Pony Punts Analytics Team are fairly sophisticated, and so the decision was made to provide them with 3 clean, denormalised Analytics tables in Redshift for them to query using SQl, or to visualise and build reports from, using a BI tool such as Tableau.

The following are the schemas for the Analytics tables;

#### Race Horses table

*Provides the Analytics team with a consolidated view of the race horses that Pony Punts customers are interested in, along with their performance in recent races, their handicap details, and form ratings*

- collected *datetime*
- market_id *decimal*
- position *decimal*
- place_paid *boolean*
- margin *decimal*
- horse_id *decimal*
- rider_id *decimal*
- handicap_weight *decimal*
- number *decimal*
- barrier *decimal*
- blinkers *boolean*
- emergency *boolean*
- form_rating_one *decimal*
- form_rating_two *decimal*
- form_rating_three *decimal*
- last_five_starts *text*
- favourite_odds_win *boolean*
- favourite_odds_place *boolean*
- favourite_pool_win *boolean*
- favourite_pool_place *boolean*
- tip_one_win *boolean*
- tip_one_place *boolean*
- tip_two_win *boolean*
- tip_two_place *boolean*
- tip_three_win *boolean*
- tip_three_place *boolean*
- tip_four_win *boolean*
- tip_four_place *boolean*
- tip_five_win *boolean*
- tip_five_place *boolean*
- tip_six_win *boolean*
- tip_six_place *boolean*
- tip_seven_win *boolean*
- tip_seven_place *boolean*
- tip_eight_win *boolean*
- tip_eight_place *boolean*
- tip_nine_win *boolean*
- tip_nine_place *boolean*
- age *decimal*
- sex_id *decimal*
- prize_money *decimal*

***

#### Customers table

*Provides the Analytics Team with details about Pony Punts' premium customers, to allow for the personalisation of service for these customers, and to give insights into their favourite tracks, horses, and odds*

- id *integer*
- bettype *text*
- date *datetime*
- tipster *text*
- track *text*
- tipsteractive *boolean*
- odds *decimal*
- result *text*

***

#### Races table

*Provides a consolidated view of individual races, including the conditions, weather, state of the ground, and the amount of money in the purse, for a given horse_id, allowing the Analytics team to analyse factors impacting the performance against multiple dimensions*

- market_id *decimal*
- horse_id *decimal*
- runner_number *decimal*
- last_twenty_starts *text*
- class_level_id *decimal*
- field_strength *decimal*
- days_since_last_run* decimal*
- runs_since_spell *boolean*
- overall_starts *decimal*
- overall_wins *decimal*
- overall_places *decimal*
- track_starts *decimal*
- track_wins *decimal*
- track_places *decimal*
- firm_starts *decimal*
- firm_wins *decimal*
- firm_places *decimal*
- good_starts *decimal*
- good_wins *decimal*
- good_places *decimal*
- dead_starts *decimal*
- dead_wins *decimal*
- dead_places *decimal*
- slow_starts *decimal*
- slow_wins *decimal*
- slow_places *decimal*
- soft_starts *decimal*
- soft_wins *decimal*
- soft_places *decimal*
- heavy_starts *decimal*
- heavy_wins *decimal*
- heavy_places *decimal*
- distance_starts *decimal,
- distance_wins *decimal*
- distance_places *decimal*
- class_same_starts *decimal*
- class_same_wins *decimal*
- class_same_places *decimal*
- class_stronger_starts *decimal*
- class_stronger_wins *decimal*
- class_stronger_places *decimal*
- first_up_starts *decimal*
- first_up_wins *decimal*
- first_up_places *decimal*
- second_up_starts *decimal*
- second_up_wins *decimal*
- second_up_places *decimal*
- track_distance_starts *decimal*
- track_distance_wins *decimal*
- track_distance_places *decimal*
- start_time *text*
- venue_id *decimal*
- race_number *decimal*
- "distance(m)" *decimal*
- condition_id *decimal*
- weather_id *decimal*
- total_pool_win_one *decimal*
- total_pool_place_one *decimal*
- total_pool_win_two *decimal*
- total_pool_place_two *decimal*
- total_pool_win_three *decimal*
- total_pool_place_three *decimal*
- weather *text*
- conditions *text*
 
 
## Future Considerations

The project involves considering how the pipeline may need to change under the following expansion scenarios;

**The data was increased by 100x.**

Due to the nature of the pipeline in using PySpark for all of the data transformations, the majority of the existing code would be fine to cope with a greatly increased dataset size.

However, currently the pipeline relies on pyodbc and boto3 to read and write CSV and JSON files from and to Amazon S3 and Redshift, and this may cause a bottleneck when dealing with big data. 

In this case, it would be worth considering moving the pipeline to make use of Amazon EMR, storing the data in S3, as at present, and processing it using Spark without relying on the intermediate I/O operations.

This would also open up the possibility of Pony Punts' Analytics Team using Spark SQL to interact with the data, instead of only using the final Redshift tables and their own BI tools.


**The pipelines would be run on a daily basis by 7 am every day.**

Currently, the pipeline must be run manually by a Data Engineer on the agreed weekly schedule.

If this schedule were to be increased, it would be worth considering the use of a tool like Airflow, to automate the scheduling of the pipleine, and crucial elements like re-tries and alerting in the case of pipeline failure.




**The database needed to be accessed by 100+ people.**

