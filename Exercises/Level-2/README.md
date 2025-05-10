#### Problems Statement
Download a file of weather data from a government website.
files that are sitting at the following specified location.

https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/

We are looking for the file that was `Last Modified` on `2024-01-19 10:27	`, We use Python to scrape
this webpage, finding the corresponding file-name for this timestamp, `2024-01-19 10:27	`

Once we have obtained the correct file, and downloaded it, we must load the file
into `Pandas` and find the record(s) with the highest `HourlyDryBulbTemperature`.
Print these record(s) to the command line.

The script does the following ...
1. Attempt to web scrap/pull down the contents of `https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/`
2. Analyze it's structure, determine how to find the corresponding file to `2024-01-19 10:27	` using Python.
3. Build the `URL` required to download this file, and write the file locally.
4. Open the file with `Pandas` and find the records with the highest `HourlyDryBulbTemperature`.
5. Print this to stdout/command line/terminal.
