"""
__author__ = "Nate Cutler"
__email__ = "ncutler211@gmail.com"
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, hour, sum, round
from pyspark import SparkFiles

def create_spark_session():
    """
    Creates a Spark session and returns it.
    """
    return SparkSession.builder.getOrCreate()

def read_votes_data(spark_session, file_url):
    """
    Reads votes data from the provided URL using Spark session and returns the DataFrame.
    
    Args:
    - spark_session: SparkSession object
    - file_url: URL containing the votes data file
    
    Returns:
    - DataFrame containing the votes data
    """
    spark_session.sparkContext.addFile(file_url)
    file_path = 'file://' + SparkFiles.get("votes_2020_hw5.txt")
    
    votes_df = (spark_session.read
                .option('header', True)
                .option('inferSchema', True)
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS")
                .csv(file_path))
    
    return votes_df

def calculate_hourly_stats(votes_df):
    """
    Calculates hourly statistics for votes data and returns a DataFrame with aggregated values.
    
    Args:
    - votes_df: DataFrame containing votes data
    
    Returns:
    - DataFrame with hourly statistics (sum of votes, biden, trump, percentages)
    """
    votes_hourly = votes_df.groupBy(dayofmonth("timestamp"), hour("timestamp"), "state") \
        .agg(sum('new_votes').alias("votes_sum"), sum('votes_biden').alias("biden_sum"),
             sum('votes_trump').alias("trump_sum"))
    
    votes_hourly = votes_hourly.withColumn('percent_biden', round(col('biden_sum') / col('votes_sum') * 100)) \
        .withColumn('percent_trump', round(col('trump_sum') / col('votes_sum') * 100))
    
    return votes_hourly

def display_dataframe(df):
    """
    Displays the content of the DataFrame.
    
    Args:
    - df: DataFrame to be displayed
    """
    df.display()

def main():
    # Create Spark session
    spark = create_spark_session()
    
    # URL containing the votes data
    data_url = "https://ista322-fall2021.s3.us-west-1.amazonaws.com/votes_2020_hw5.txt"
    
    # Read votes data
    votes_data = read_votes_data(spark, data_url)
    
    # Calculate hourly statistics
    hourly_stats = calculate_hourly_stats(votes_data)
    
    # Display hourly statistics
    display_dataframe(hourly_stats)
    
    # Dropping and creating a global temporary view
    spark.catalog.dropGlobalTempView("votes_hourly")
    hourly_stats.createGlobalTempView("votes_hourly")
    
    # SQL queries on the global temporary view
    query_1 = """SELECT * FROM global_temp.votes_hourly
              WHERE state = 'Arizona (EV: 11)'"""
    spark.sql(query_1).display()
    
    query_2 = """SELECT `dayofmonth(timestamp)` AS day, state, SUM(trump_sum)/SUM(votes_sum)*100 AS daily_percent_trump
              FROM global_temp.votes_hourly
              GROUP BY `dayofmonth(timestamp)`, state
              SORT BY state;"""
    spark.sql(query_2).display()

if __name__ == "__main__":
    main()
