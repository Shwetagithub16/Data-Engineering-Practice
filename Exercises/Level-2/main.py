import requests
import pandas as pd
import os
from bs4 import BeautifulSoup

#Task:
#Web scrap a HTML page looking for a date, and identifying the correct file to build a URL with which you can download said file

def main():
    # your code here
    link = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

    def get_filename():
        # find a find which was last modified on "2024-01-19 10:27"
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")
        # print(soup.prettify())
        table = soup.find("table")
        df = pd.read_html(str(table))[0]
        filtered_df = df[(df["Last modified"]=="2024-01-19 10:27") & (df["Size"]=="1.0M")]
        filename = filtered_df["Name"].iloc[0]
        download_url = link + filename
        print("URL and Filename fetched")
        print(f"URL: {download_url}")
        print(f"Filename:{filename}")
        return download_url, filename

    def download_file(download_url, filename):

        response = requests.get(download_url)
        download_file_path = os.path.join(os.getcwd(),filename)


        if response.status_code==200:
            with open(download_file_path, "wb") as file:
                file.write(response.content)
            print(f"File downloaded:{download_file_path}")
            df = pd.read_csv(filename)
            print(df[df["HourlyDryBulbTemperature"]==df["HourlyDryBulbTemperature"].max()])
        else:
            print("Failed to download the file, HTTP Status:", response.status_code)

    download_url, filename = get_filename()
    download_file(download_url, filename)


if (__name__ == "__main__"):
    main()
