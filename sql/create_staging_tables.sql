-- Create staging tables

CREATE TABLE IF NOT EXISTS staging_riders (
    id int,
    sex text
);


CREATE TABLE IF NOT EXISTS staging_weather (
    id integer,
    name text
);

            
CREATE TABLE IF NOT EXISTS staging_horses (
    id integer,
    age integer,
    sex_id integer,
    sire_id integer,
    dam_id integer,
    prize_money decimal
);


CREATE TABLE IF NOT EXISTS staging_horse_sexes (
    id integer,
    name text
);


CREATE TABLE IF NOT EXISTS staging_conditions (
    id integer,
    name text
);


CREATE TABLE IF NOT EXISTS staging_forms (
    collected datetime,
    market_id integer,
    horse_id integer,
    runner_number integer,
    last_twenty_starts text,
    class_level_id integer,
    field_strength decimal,
    days_since_last_run integer,
    runs_since_spell boolean,
    overall_starts integer,
    overall_wins integer,
    overall_places integer,
    track_starts integer,
    track_wins integer,
    track_places integer,
    firm_starts integer,
    firm_wins integer,
    firm_places integer,
    good_starts integer,
    good_wins integer,
    good_places integer,
    dead_starts integer,
    dead_wins integer,
    dead_places integer,
    slow_starts integer,
    slow_wins integer,
    slow_places integer,
    soft_starts integer,
    soft_wins integer,
    soft_places integer,
    heavy_starts integer,
    heavy_wins integer,
    heavy_places integer,
    distance_starts integer,
    distance_wins integer,
    distance_places integer,
    class_same_starts integer,
    class_same_wins integer,
    class_same_places integer,
    class_stronger_starts integer,
    class_stronger_wins integer,
    class_stronger_places integer,
    first_up_starts integer,
    first_up_wins integer,
    first_up_places integer,
    second_up_starts integer,
    second_up_wins integer,
    second_up_places integer,
    track_distance_starts integer,
    track_distance_wins integer,
    track_distance_places integer
);


CREATE TABLE IF NOT EXISTS staging_odds (
    runner_id integer,
    collected datetime,
    odds_one_win decimal,
    odds_one_win_wagered decimal,
    odds_one_place decimal,
    odds_one_place_wagered decimal,
    odds_two_win decimal,
    odds_two_win_wagered decimal,
    odds_two_place decimal,
    odds_two_place_wagered decimal,
    odds_three_win decimal,
    odds_three_win_wagered decimal,
    odds_three_place decimal,
    odds_three_place_wagered decimal,
    odds_four_win decimal,
    odds_four_win_wagered decimal,
    odds_four_place decimal,
    odds_four_place_wagered decimal
);


CREATE TABLE IF NOT EXISTS staging_runners (
    id integer,
    collected datetime,
    market_id integer,
    position integer,
    place_paid boolean,
    margin decimal,
    horse_id integer,
    trainer_id integer,
    rider_id integer,
    handicap_weight decimal,
    number integer,
    barrier integer,
    blinkers boolean,
    emergency boolean,
    form_rating_one decimal,
    form_rating_two decimal,
    form_rating_three decimal,
    last_five_starts text,
    favourite_odds_win boolean,
    favourite_odds_place boolean,
    favourite_pool_win boolean,
    favourite_pool_place boolean,
    tip_one_win boolean,
    tip_one_place boolean,
    tip_two_win boolean,
    tip_two_place boolean,
    tip_three_win boolean,
    tip_three_place boolean,
    tip_four_win boolean,
    tip_four_place boolean,
    tip_five_win boolean,
    tip_five_place boolean,
    tip_six_win boolean,
    tip_six_place boolean,
    tip_seven_win boolean,
    tip_seven_place boolean,
    tip_eight_win boolean,
    tip_eight_place boolean,
    tip_nine_win boolean,
    tip_nine_place boolean
);


CREATE TABLE IF NOT EXISTS staging_markets (
    id integer,
    start_time text, 
    venue_id integer,
    race_number integer,
    "distance(m)" integer,
    condition_id integer,
    weather_id integer,
    total_pool_win_one decimal,
    total_pool_place_one decimal,
    total_pool_win_two decimal,
    total_pool_place_two decimal,
    total_pool_win_three decimal,
    total_pool_place_three decimal
);