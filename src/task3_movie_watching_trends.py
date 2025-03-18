from pyspark.sql import SparkSession  
from pyspark.sql.functions import col, count

def initialize_spark(app_name="Task3_Trend_Analysis"):
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

def analyze_trend_over_years(df):
    """
    Analyze movie-watching trends over the years and identify peak years.
    """
    trend_analysis_df = df.groupBy("WatchedYear").agg(count("MovieID").alias("Movies Watched"))
    return trend_analysis_df.orderBy("WatchedYear")

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 3.
    """
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"  # Updated path based on your directory structure
    df = load_data(spark, input_file)
    
    # Task 3: Trend Analysis Over the Years
    trend_result = analyze_trend_over_years(df)
    write_output(trend_result, "Outputs/movie_watching_trends.csv")
    
    spark.stop()

if __name__ == "__main__":
    main()