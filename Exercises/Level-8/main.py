import polars as pl

#Task:
#use the Lazy functionality of Polars to work on this data in a efficient manner.
# Convert all data types to the correct ones.
# Count the number bike rides per day.
# Calculate the average, max, and minimum number of rides per week of the dataset.
# For each day, calculate how many rides that day is above or below the same day last week.

def main():
    # df = pl.scan_csv("./data/202306-divvy-tripdata.csv")

    schema_overrides = {
        "ride_id": pl.Utf8,
        "rideable_type": pl.Utf8,
        "started_at": pl.Utf8,  # Will parse later
        "ended_at": pl.Utf8,  # Will parse later
        "start_station_name": pl.Utf8,
        "start_station_id": pl.Utf8,  # Ensure IDs are treated as strings
        "end_station_name": pl.Utf8,
        "end_station_id": pl.Utf8,  # Ensure IDs are treated as strings
        "start_lat": pl.Float64,
        "start_lng": pl.Float64,
        "end_lat": pl.Float64,
        "end_lng": pl.Float64,
        "member_casual": pl.Utf8
    }

    # Read CSV lazily with schema overrides
    lf = pl.scan_csv(
        "./data/202306-divvy-tripdata.csv",
        schema_overrides=schema_overrides
    )

    # Show first few rows safely
    # Convert started_at to Date format and count rides per day
    # Convert started_at to Date format correctly
    rides_per_day = (
        lf.with_columns(
            pl.col("started_at").str.to_datetime("%Y-%m-%d %H:%M:%S").dt.date().alias("date")
        )
        .group_by("date")
        .agg(pl.len().alias("ride_count"))  # Use `pl.len()` instead of `pl.count()`
    )

    # Extract the week number and year for grouping
    weekly_stats = (
        rides_per_day.with_columns(
            pl.col("date").dt.week().alias("week"),
            pl.col("date").dt.year().alias("year")
        )
        .group_by(["year", "week"])
        .agg(
            pl.mean("ride_count").alias("avg_rides"),
            pl.max("ride_count").alias("max_rides"),
            pl.min("ride_count").alias("min_rides")
        )
    )

    # Join rides per day with itself (offset by 7 days)
    rides_vs_last_week = (
        rides_per_day.join(
            rides_per_day.with_columns((pl.col("date") + pl.duration(days=7)).alias("date")),
            on="date",
            suffix="_last_week"
        )
        .with_columns((pl.col("ride_count") - pl.col("ride_count_last_week")).alias("diff_from_last_week"))
    )

    print("Rides per Day:")
    print(rides_per_day.collect())

    print("\nWeekly Ride Stats:")
    print(weekly_stats.collect())

    print("\nDay-to-Day Difference from Last Week:")
    print(rides_vs_last_week.collect())


if __name__ == "__main__":
    main()
