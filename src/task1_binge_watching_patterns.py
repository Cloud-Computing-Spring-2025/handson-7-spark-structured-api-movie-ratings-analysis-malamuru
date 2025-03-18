from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round as spark_round

def initialize_spark(app_name="Task1_Binge_Watching_Patterns"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the movie ratings data from a CSV file into a Spark DataFrame.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def detect_binge_watching_patterns(df):
    """
    Identify the percentage of users in each age group who binge-watch movies.
    """
    binge_watchers_df = df.filter(col("IsBingeWatched") == True)
    binge_counts = binge_watchers_df.groupBy("AgeGroup").agg(count("UserID").alias("Binge Watchers"))
    total_users = df.groupBy("AgeGroup").agg(count("UserID").alias("Total Users"))
    binge_watch_summary = binge_counts.join(total_users, "AgeGroup")\
        .withColumn("Percentage", spark_round((col("Binge Watchers") / col("Total Users")) * 100, 2))
    return binge_watch_summary

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 1.
    """
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"  # Updated path based on your directory structure
    df = load_data(spark, input_file)
    
    # Task 1: Binge Watching Patterns
    binge_result = detect_binge_watching_patterns(df)
    write_output(binge_result, "Outputs/binge_watching_patterns.csv")
    
    spark.stop()

if __name__ == "__main__":
    main()
