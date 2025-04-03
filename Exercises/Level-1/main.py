import os
import aiohttp
import asyncio
import zipfile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

#Task:
#File handling using python

# Define the downloads directory
downloads = os.path.join(os.getcwd(), "downloads")


def create_directory():
    """Create the downloads directory if it does not exist."""
    if not os.path.exists(downloads):
        os.mkdir(downloads)
        print(f"Directory '{downloads}' created")
    else:
        print(f"Directory already exists")


async def download_file(session, fileurl):
    """Download a ZIP file asynchronously and extract its CSV contents."""
    filename = os.path.basename(fileurl)  # Extract filename from URL
    zip_path = os.path.join(downloads, filename)  # Path to save ZIP file

    async with session.get(fileurl) as response:
        if response.status == 200:
            # Save ZIP file
            with open(zip_path, 'wb') as f:
                f.write(await response.read())
            print(f"Downloaded: {filename}")

            # Extract CSV files
            extract_zip(zip_path)

            # Delete the ZIP file after extraction
            os.remove(zip_path)
            print(f"Deleted ZIP file: {filename}")
        else:
            print(f"Failed to download {filename}. HTTP Status Code: {response.status}")


def extract_zip(zip_path):
    """Extract CSV files from the given ZIP file and delete the ZIP after extraction."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(downloads)  # Extract all CSV files to downloads folder
            extracted_files = zip_ref.namelist()
            print(f"Extracted: {', '.join(extracted_files)}")
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid ZIP file.")


async def download_files():
    """Download all ZIP files asynchronously."""
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url) for url in download_uris]
        await asyncio.gather(*tasks)


def main():
    create_directory()  # Ensure downloads folder exists
    asyncio.run(download_files())  # Download and extract files asynchronously


if __name__ == "__main__":
    main()
