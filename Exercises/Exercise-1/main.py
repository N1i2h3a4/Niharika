import aiohttp
import asyncio
import zipfile
from concurrent.futures import ThreadPoolExecutor
import os

async def download_and_extract_file(session, url, extract_path):
    async with session.get(url) as response:
        if response.status != 200:
            print(f"Skipping {url} - Failed to download")
            return

        save_path = os.path.join(extract_path, os.path.basename(url))
        with open(save_path, 'wb') as file:
            while True:
                chunk = await response.content.read(1024)
                if not chunk:
                    break
                file.write(chunk)
        
        # Extract the downloaded zip file
        with zipfile.ZipFile(save_path, 'r') as zip_file:
            zip_file.extractall(extract_path)
        
        # Remove the downloaded zip file
        os.remove(save_path)

async def download_and_extract_files_async(urls, extract_path):
    connector = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for url in urls:
            tasks.append(download_and_extract_file(session, url, extract_path))
        await asyncio.gather(*tasks)

def main(urls, extract_path):
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(download_and_extract_files_async(urls, extract_path))
    executor.shutdown()

# Example usage:
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

extract_path = 'downloads'

if __name__ == "__main__":
# Create the 'downloads' folder if it doesn't exist
    if not os.path.exists(extract_path):
        os.makedirs(extract_path)

    main(download_uris, extract_path)
    print("=======ACTION COMPLETED========")