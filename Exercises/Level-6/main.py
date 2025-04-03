from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rank, avg, sum, to_date, regexp_replace, expr, to_timestamp, count, month, desc, date_sub, row_number,max
from pyspark.sql.window import Window
import warnings
import logging
warnings.filterwarnings("ignore")
from datetime import datetime
import zipfile
import os
import sys

#Task:
#Aggregations and calculations with Pyspark
# What is the average trip duration per day?
# How many trips were taken each day?
# What was the most popular starting trip station for each month?
# What were the top 3 trip stations each day for the last two weeks?
# Do Males or Females take longer trips on average?
# What is the top 10 ages of those that take the longest trips, and shortest?

sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Options: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"

    # Suppress INFO and DEBUG logs from the root logger
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)
    zip_file_path = "./data/Divvy_Trips_2019_Q4.zip"
    extract_folder = "./data/extracted"


    def save_with_filename(df, folder_path, filename):
        """
        Saves a DataFrame as a CSV file with a specific filename.
        """
        temp_path = folder_path + "_temp"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)

        # Find the actual written CSV file
        csv_file = None
        for file in os.listdir(temp_path):
            if file.endswith(".csv"):
                csv_file = file
                break

        # Rename and move the file
        if csv_file:
            os.rename(os.path.join(temp_path, csv_file), os.path.join(folder_path, filename))

        # Remove temporary folder
        for file in os.listdir(temp_path):
            os.remove(os.path.join(temp_path, file))
        os.rmdir(temp_path)

        print(f"✅ Results saved as {folder_path}/{filename}")

    # Ensure the extraction directory exists
    os.makedirs(extract_folder, exist_ok=True)

    def extract_zip(zip_file_path, extract_folder):
        # Extract ZIP file
        with zipfile.ZipFile(zip_file_path, "r") as z:
            z.extractall(extract_folder)

        # Get the extracted CSV file name (assuming one CSV file in ZIP)
        extracted_files = [f for f in os.listdir(extract_folder) if f.endswith(".csv")]
        if not extracted_files:
            raise FileNotFoundError("No CSV file found in the extracted ZIP archive.")

        csv_file_path = os.path.join(extract_folder, extracted_files[0])  # Use first CSV file

        # Check if the file is empty
        if os.stat(csv_file_path).st_size == 0:
            raise ValueError("Extracted CSV file is empty.")

        # Read the CSV file with Spark
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_file_path)

        return df

    def average_trip_duration_per_day(df):

        # df.select("start_time").show(5, truncate=False)
        df = df.withColumn("date", to_date(col("start_time")))
        # df.select("date").show(5, truncate=False)
        df = df.withColumn("tripduration", regexp_replace(col("tripduration"),",","").cast("double"))

        avg_trip_duration = df.groupBy("date").agg(avg("tripduration").alias("avgtripduration"))

        save_with_filename(avg_trip_duration, "./data/output", "average_trip_duration.csv")


    def trips_each_day(df):

        df = df.withColumn("date", to_date(col("start_time")))


        trips_per_day = df.groupBy("date").agg(count("trip_id").alias("num_trips"))
        save_with_filename(trips_per_day, "./data/output", "trips_each_day.csv")


    def most_popular_start_station(df):

        df = df.withColumn("date", to_date(col("start_time")))
        df = df.withColumn("month", month(col("date")))

        station_count = df.groupBy("month","from_station_name").agg(count("*").alias("trip_count"))
        window_spec = Window.partitionBy("month").orderBy(desc("trip_count"))
        top_stations = station_count.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")
        save_with_filename(top_stations, "./data/output", "most_popular_start_station.csv")

    def top_three_stations(df):
        df = df.withColumn("date", to_date(col("start_time")))
        max_date = df.select(max(col("date"))).collect()[0][0]

        df_filtered = df.filter(col("date") > date_sub(lit(max_date), 14))

        station_counts = df_filtered.groupBy("date", "from_station_name") \
            .agg(count("*").alias("trip_count"))

        # Define window partitioned by date, ordered by trip_count descending
        window_spec = Window.partitionBy("date").orderBy(desc("trip_count"))

        # Rank stations within each day and filter the top 3
        top_stations = station_counts.withColumn("rank", rank().over(window_spec)) \
            .filter(col("rank") <= 3).drop("rank")

        save_with_filename(top_stations, "./data/output", "top_three_stations.csv")

    def male_female_trip(df):
        df = df.withColumn("tripduration", regexp_replace(col("tripduration"), ",", "").cast("double"))
        # df.select("tripduration").show(5, truncate=False)
        gender_duration = df.groupBy("gender").agg(avg("tripduration").alias("genderduration"))
        notnull_gender = gender_duration.filter(col("gender").isNotNull())
        max_gender = notnull_gender.orderBy(col("genderduration").desc()).limit(1)

        save_with_filename(max_gender, "./data/output", "male_female_trip.csv")


    def top_10_ages(df):
        current_year = datetime.now().year

        df = df.withColumn("birthyear", col("birthyear").cast("int"))
        df = df.withColumn("age", current_year-col("birthyear"))
        df = df.withColumn("tripduration", regexp_replace(col("tripduration"), ",", "").cast("double"))
        # df.select("age").show(5, truncate=False)
        df_filtered = df.filter(col("age").isNotNull())
        top_10_ages_longesttrip = df_filtered.orderBy(col("tripduration").desc()).limit(10)
        top_10_ages_shoetesttrip = df_filtered.orderBy(col("tripduration")).limit(10)
        save_with_filename(top_10_ages_longesttrip, "./data/output", "top_10_ages_longesttrip.csv")
        save_with_filename(top_10_ages_shoetesttrip, "./data/output", "top_10_ages_shoetesttrip.csv")



    df = extract_zip(zip_file_path, extract_folder)


    average_trip_duration_per_day(df)
    trips_each_day(df)
    most_popular_start_station(df)
    top_three_stations(df)
    male_female_trip(df)
    top_10_ages(df)
    top_10_ages(df)
    # your code here



if __name__ == "__main__":
    main()
