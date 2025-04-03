from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract, lit, split, when, col, desc, sha2, concat_ws, dense_rank
from pyspark.sql.window import Window
import warnings
import logging
warnings.filterwarnings("ignore")

#Task:
#Aggregation and Calculations using pyspark.

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Options: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"

    # Suppress INFO and DEBUG logs from the root logger
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.ERROR)
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    df = spark.read.option("header", "true").csv("./data/hard-drive-2022-01-01-failures.csv")
    # df.show(1, truncate=False)

    def add_filename(df):
        df = df.withColumn("source_file", regexp_extract(input_file_name(), r"([^/]+$)", 1))
        list = df.select("source_file").distinct().collect()
        filename = list[0][0]
        # print(type(filename))

        return filename

    def add_date(df,filename):
        parts = filename.split("-")
        extracted_date = f"{parts[2]}-{parts[3]}-{parts[4].split('.')[0]}"
        # print(extracted_date)
        df = df.withColumn("file_date",lit(extracted_date))

    def add_brand(df):
        df = df.withColumn(
            "brand",
            when(col("model").contains(" "), split(col("model"), " ")[0])  # Extract first word if space exists
            .otherwise("unknown")  # Assign "unknown" if no space is found
        )

        # df.select("model", "brand").show(10, truncate=False)
    def add_rank(df):
        window = Window.orderBy(desc('capacity_bytes'))
        df = df.withColumn('storage_ranking', dense_rank().over(window))
        df.select("storage_ranking","capacity_bytes", "model").show(10, truncate=False)

    def add_primary_key(df):
        # Columns that uniquely identify a record
        unique_columns = ["date", "serial_number", "model"]

        # Create a hash-based primary key
        df = df.withColumn("primary_key", sha2(concat_ws("_", *unique_columns), 256))


    filename = add_filename(df)
    add_date(df,filename)
    add_brand(df)
    add_rank(df)
    add_primary_key(df)

if __name__ == "__main__":
    main()
