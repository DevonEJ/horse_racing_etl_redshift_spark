-- Copy data from S3 to Redshift staging tables

COPY race_horses FROM 's3://udacity-horse-racing/race_horses.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
CSV
ACCEPTANYDATE
DELIMITER ',';


COPY customers FROM 's3://udacity-horse-racing/customers.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
CSV
ACCEPTANYDATE
DELIMITER ',';


COPY races FROM 's3://udacity-horse-racing/races.csv'
iam_role 'arn:aws:iam::718240196138:role/myRedshiftRole'
region 'eu-west-2'
IGNOREHEADER 1
CSV
ACCEPTANYDATE
DELIMITER ',';