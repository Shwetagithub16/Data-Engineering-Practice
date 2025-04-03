import requests

#Task:
#download a .gz file located in s3 bucket
#extract the file, open it, and download the file uri located on the first line
#Store the file locally and iterate through the lines of the file, printing each line to stdout

def main():

    # S3 Bucket and Key
    base_url = "https://data.commoncrawl.org/"
    wet_paths_url = base_url + "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

    print(f"Downloading: {wet_paths_url}")
    response = requests.get(wet_paths_url)
    with open("wet.paths.gz", "wb") as f:
        f.write(response.content)

    # Step 2: Extract and get the first file path
    import gzip

    with gzip.open("wet.paths.gz", "rt") as f:
        first_wet_file_path = f.readline().strip()

    print(f"First WET file path: {first_wet_file_path}")

    # Step 3: Download the first WET file
    wet_file_url = base_url + first_wet_file_path
    print(f"Downloading: {wet_file_url}")

    response = requests.get(wet_file_url, stream=True)
    with open("first_wet_file.warc.wet.gz", "wb") as f:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)

    print("Download complete. Extracting...")

    with gzip.open("first_wet_file.warc.wet.gz", "rt") as f:
        for i, line in enumerate(f):
            print(line.strip())
            if i >= 20:  # Print only first 20 lines
                break




if __name__ == "__main__":
    main()
