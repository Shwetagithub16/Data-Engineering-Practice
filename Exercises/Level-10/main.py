from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

#Task:
#Recently the analytics that have been calculating the max bike trip durations have shown some very strange and long trip durations. It's expected that most bike trips start and end on the same day. We need to implement a Data Quality alert that will let us know when we are getting erroneous durations in ride times.

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BikeRideDuration") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

input_csv_path = "data/202306-divvy-tripdata.csv"

# Read CSV with schema
df = spark.read.csv(
    input_csv_path,
    header=True,
    schema=schema,
    mode="DROPMALFORMED"
)

# Convert timestamps
df = df.withColumn("started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss"))

# Calculate ride duration
df = df.withColumn("duration_seconds", unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at")))

# Filter out negative or excessively long durations (> 24 hours)
df = df.filter((col("duration_seconds") > 0) & (col("duration_seconds") <= 86400))

# Extract date for aggregation
df = df.withColumn("date", date_format(col("started_at"), "yyyy-MM-dd"))

# Aggregate daily ride durations
daily_durations = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)

# Convert Spark DataFrame to Pandas
df_pandas = daily_durations.toPandas()

# Initialize Great Expectations context
context = gx.get_context()

# Create an in-memory batch request
batch_request = RuntimeBatchRequest(
    datasource_name="pandas_datasource",
    data_connector_name="runtime_data_connector",
    data_asset_name="bike_trips",
    runtime_parameters={"batch_data": df_pandas},
    batch_identifiers={"pipeline_stage": "validation"},
)

# Get a validator
validator = context.get_validator(batch_request=batch_request)

# Define expectations
validator.expect_column_values_to_be_between(
    column="total_duration_seconds",
    min_value=60,  # At least 1 minute
    max_value=86400  # No more than 24 hours
)

# Validate dataset
results = validator.validate()
print(results)

# Save results
output_parquet_path = "results/output_file.parquet"
daily_durations.write.mode("overwrite").parquet(output_parquet_path)
