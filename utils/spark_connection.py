from pyspark.sql import SparkSession


def create_spark_session():
    """
    Returns spark session object with the ability to interact with Amazon S3.
    """
    
    spark = SparkSession \
        .builder \
        .appName('Horse-Racing-Analytics-Table-Pipeline') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    return spark