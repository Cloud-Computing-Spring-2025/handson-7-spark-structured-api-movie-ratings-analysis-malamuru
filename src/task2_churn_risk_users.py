from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, count, lit

def initialize_spark(app_name="Task2_Churn_Risk_Users"):
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

def identify_churn_risk_users(df):
    """
    Identify users with canceled subscriptions and low watch time (<100 minutes).
    """
    churn_risk_df = df.filter((col("SubscriptionStatus") == "Canceled") & (col("WatchTime") < 100))
    churn_risk_users= churn_risk_df.agg(count("UserID").alias("Total Users"))
    
    # Adding the required label column
    churn_risk_users = churn_risk_users.withColumn("Churn Risk Users", lit("Users with low watch time & canceled subscriptions"))
    
    return churn_risk_users.select("Churn Risk Users", "Total Users")

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

def main():
    """
    Main function to execute Task 2.
    """
    spark = initialize_spark()
    input_file = "input/movie_ratings_data.csv"  # Updated path based on your directory structure
    df = load_data(spark, input_file)
    
    # Task 2: Identify Churn Risk Users
    churn_result = identify_churn_risk_users(df)
    write_output(churn_result, "Outputs/churn_risk_users.csv")
    
    spark.stop()

if __name__ == "__main__":
    main()

