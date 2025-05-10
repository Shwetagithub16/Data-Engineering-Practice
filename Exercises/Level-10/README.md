

#### Problems Statement
There is a folder called `data` in this current directory. Inside this
folder there is a `csv` file. The file is called `202306-divvy-tripdate.csv`. This is an open source
data set bike trips.

Generally the files look like this ...
```
"ride_id","rideable_type","started_at","ended_at","start_station_name","start_station_id","end_station_name","end_station_id","start_lat","start_lng","end_lat","end_lng","member_casual"
"6F1682AC40EB6F71","electric_bike","2023-06-05 13:34:12","2023-06-05 14:31:56",,,,,41.91,-87.69,41.91,-87.7,"member"
```

Recently the analytics that have been calculating the max bike trip durations have shown some very strange
and long trip durations. It's expected that most bike trips start and end on the same day. We need to 
implement a Data Quality alert that will let us know when we are getting erroneous durations in ride times.

1. Use Great Expectations to satisfy this requirement.
2. The current dataset includes erroneous trip lenghts, when you run this pipeline (using `docker-compose up run`) 
your data quality checks should pick up on this.



