# Contains a schema for each of the PySpark dataframes created from the staging datasets

from pyspark.sql.types import *


def staging_riders_schema() -> StructType:
    schema = StructType([ StructField("id", IntegerType(), False)\
                          ,StructField("sex", StringType(), True)])
    return schema
  
    
def staging_weather_schema():
    schema = StructType([ StructField("id", IntegerType(), False)\
                          ,StructField("name", StringType(), True)])
    return schema
     
    
def staging_horses_schema() -> StructType:                                     
    schema = StructType([ StructField("id", IntegerType(), False)
                                    ,StructField("age", DoubleType(), True)
                                    ,StructField("sex_id", DoubleType(), True)
                                    ,StructField("sire_id", DoubleType(), True)
                                    ,StructField("dam_id", DoubleType(), True)
                                    ,StructField("prize_money", DecimalType(), True)])
    return schema
                                    

def staging_horse_sexes_schema() -> StructType:
    schema = StructType([ StructField("id", IntegerType(), False)\
                          ,StructField("name", StringType(), True)])
    return schema
           

def staging_conditions_schema() -> StructType:
    schema = StructType([ StructField("id", IntegerType(), False)\
                          ,StructField("name", StringType(), True)]) 
    return schema
                                        

def staging_forms_schema() -> StructType:
    schema = StructType([ StructField("collected", DateType(), False)\
                         ,StructField("market_id", IntegerType(), False)\
                         ,StructField("horse_id", IntegerType(), False)\
                         ,StructField("runner_number", IntegerType(), True)\
                         ,StructField("last_twenty_starts", StringType(), True)\
                         ,StructField("class_level_id", IntegerType(), True)\
                         ,StructField("field_strength", DecimalType(), True)\
                         ,StructField("days_since_last_run", IntegerType(), True)\
                         ,StructField("runs_since_spell", BooleanType(), True)\
                         ,StructField("overall_starts", IntegerType(), True)\
                         ,StructField("overall_wins", IntegerType(), True)\
                         ,StructField("overall_places", IntegerType(), True)\
                         ,StructField("track_starts", IntegerType(), True)\
                         ,StructField("track_wins", IntegerType(), True)\
                         ,StructField("track_places", IntegerType(), True)\
                         ,StructField("firm_starts", IntegerType(), True)\
                         ,StructField("firm_wins", IntegerType(), True)\
                         ,StructField("firm_places", IntegerType(), True)\
                         ,StructField("good_starts", IntegerType(), True)\
                         ,StructField("good_wins", IntegerType(), True)\
                         ,StructField("good_places", IntegerType(), True)\
                         ,StructField("dead_starts", IntegerType(), True)\
                         ,StructField("dead_wins", IntegerType(), True)\
                         ,StructField("dead_places", IntegerType(), True)\
                         ,StructField("slow_starts", IntegerType(), True)\
                         ,StructField("slow_wins", IntegerType(), True)\
                         ,StructField("slow_places", IntegerType(), True)\
                         ,StructField("soft_starts", IntegerType(), True)\
                         ,StructField("soft_wins", IntegerType(), True)\
                         ,StructField("soft_places", IntegerType(), True)\
                         ,StructField("heavy_starts", IntegerType(), True)\
                         ,StructField("heavy_wins", IntegerType(), True)\
                         ,StructField("heavy_places", IntegerType(), True)\
                         ,StructField("distance_starts", IntegerType(), True)\
                         ,StructField("distance_wins", IntegerType(), True)\
                         ,StructField("distance_places", IntegerType(), True)\
                         ,StructField("class_same_starts", IntegerType(), True)\
                         ,StructField("class_same_wins", IntegerType(), True)\
                         ,StructField("class_same_places", IntegerType(), True)\
                         ,StructField("class_stronger_starts", IntegerType(), True)\
                         ,StructField("class_stronger_wins", IntegerType(), True)\
                         ,StructField("class_stronger_places", IntegerType(), True)\
                         ,StructField("first_up_starts", IntegerType(), True)\
                         ,StructField("first_up_wins", IntegerType(), True)\
                         ,StructField("first_up_places", IntegerType(), True)\
                         ,StructField("second_up_starts", IntegerType(), True)\
                         ,StructField("second_up_wins", IntegerType(), True)\
                         ,StructField("second_up_places", IntegerType(), True)\
                         ,StructField("track_distance_starts", IntegerType(), True)\
                         ,StructField("track_distance_wins", IntegerType(), True)\
                         ,StructField("track_distance_places", IntegerType(), True)])
    return schema


def staging_odds_schema() -> StructType:
    schema = StructType([ StructField("runner_id", IntegerType(), False)\
                         ,StructField("collected", DateType(), True)\
                         ,StructField("odds_one_win", DecimalType(), True)\
                         ,StructField("odds_one_win_wagered", DecimalType(), True)\
                         ,StructField("odds_one_place", DecimalType(), True)\
                         ,StructField("odds_one_place_wagered", DecimalType(), True)\
                         ,StructField("odds_two_win", DecimalType(), True)\
                         ,StructField("odds_two_win_wagered", DecimalType(), True)\
                         ,StructField("odds_two_place", DecimalType(), True)\
                         ,StructField("odds_two_place_wagered", DecimalType(), True)\
                         ,StructField("odds_three_win", DecimalType(), True)\
                         ,StructField("odds_three_win_wagered", DecimalType(), True)\
                         ,StructField("odds_three_place", DecimalType(), True)\
                         ,StructField("odds_three_place_wagered", DecimalType(), True)\
                         ,StructField("odds_four_win", DecimalType(), True)\
                         ,StructField("odds_four_win_wagered", DecimalType(), True)\
                         ,StructField("odds_four_place", DecimalType(), True)\
                         ,StructField("odds_four_place_wagered", DecimalType(), True)])
    return schema


def staging_runners_schema() -> StructType:
    schema = StructType([ StructField("id", IntegerType(), False)\
                         ,StructField("collected", DateType(), True)\
                         ,StructField("market_id", IntegerType(), True)\
                         ,StructField("position", DoubleType(), True)\
                         ,StructField("place_paid", BooleanType(), True)\
                         ,StructField("margin", DecimalType(), True)\
                         ,StructField("horse_id", IntegerType(), True)\
                         ,StructField("trainer_id", DoubleType(), True)\
                         ,StructField("rider_id", DoubleType(), True)\
                         ,StructField("handicap_weight", DecimalType(), True)\
                         ,StructField("number", IntegerType(), True)\
                         ,StructField("barrier", IntegerType(), True)\
                         ,StructField("blinkers", BooleanType(), True)\
                         ,StructField("emergency", BooleanType(), True)\
                         ,StructField("form_rating_one", DecimalType(), True)\
                         ,StructField("form_rating_two", DecimalType(), True)\
                         ,StructField("form_rating_three", DecimalType(), True)\
                         ,StructField("last_five_starts", StringType(), True)\
                         ,StructField("favourite_odds_win", BooleanType(), True)\
                         ,StructField("favourite_odds_place", BooleanType(), True)\
                         ,StructField("favourite_pool_win", BooleanType(), True)\
                         ,StructField("favourite_pool_place", BooleanType(), True)\
                         ,StructField("tip_one_win", BooleanType(), True)\
                         ,StructField("tip_one_place", BooleanType(), True)\
                         ,StructField("tip_two_win", BooleanType(), True)\
                         ,StructField("tip_two_place", BooleanType(), True)\
                         ,StructField("tip_three_win", BooleanType(), True)\
                         ,StructField("tip_three_place", BooleanType(), True)\
                         ,StructField("tip_four_win", BooleanType(), True)\
                         ,StructField("tip_four_place", BooleanType(), True)\
                         ,StructField("tip_five_win", BooleanType(), True)\
                         ,StructField("tip_five_place", BooleanType(), True)\
                         ,StructField("tip_six_win", BooleanType(), True)\
                         ,StructField("tip_six_place", BooleanType(), True)\
                         ,StructField("tip_seven_win", BooleanType(), True)\
                         ,StructField("tip_seven_place", BooleanType(), True)\
                         ,StructField("tip_eight_win", BooleanType(), True)\
                         ,StructField("tip_eight_place", BooleanType(), True)\
                         ,StructField("tip_nine_win", BooleanType(), True)\
                         ,StructField("tip_nine_place", BooleanType(), True)])
    return schema


def staging_markets_schema() -> StructType:
    schema = StructType([ StructField("id", IntegerType(), False)\
                         ,StructField("start_time", StringType(), True)\
                         ,StructField("venue_id", IntegerType(), True)\
                         ,StructField("race_number", IntegerType(), True)\
                         ,StructField("distance(m)", IntegerType(), True)\
                         ,StructField("condition_id", IntegerType(), True)\
                         ,StructField("weather_id", IntegerType(), True)\
                         ,StructField("total_pool_win_one", DecimalType(), True)\
                         ,StructField("total_pool_place_one", DecimalType(), True)\
                         ,StructField("total_pool_win_two", DecimalType(), True)\
                         ,StructField("total_pool_place_two", DecimalType(), True)\
                         ,StructField("total_pool_win_three", DecimalType(), True)\
                         ,StructField("total_pool_place_three", DecimalType(), True)])
    return schema
