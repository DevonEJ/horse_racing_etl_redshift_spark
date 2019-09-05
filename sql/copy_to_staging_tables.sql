-- Copy data from S3 to Redshift staging tables

COPY staging_riders FROM 's3://udacity-horse-racing/data/riders.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_weather FROM 's3://udacity-horse-racing/data/weathers.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_forms FROM 's3://udacity-horse-racing/data/forms.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_horses FROM 's3://udacity-horse-racing/data/horses.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_horse_sexes FROM 's3://udacity-horse-racing/data/horse_sexes.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';

COPY staging_markets FROM 's3://udacity-horse-racing/data/markets.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_odds FROM 's3://udacity-horse-racing/data/odds.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_conditions FROM 's3://udacity-horse-racing/data/conditions.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';


COPY staging_runners FROM 's3://udacity-horse-racing/data/runners.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
DELIMITER ',';
