import requests
from bs4 import BeautifulSoup
import pandas as pd

# Step 1: Web scraping to find the file name
url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

# Find the file corresponding to the given timestamp
target_timestamp = "2022-02-07 14:03"
file_name = None

# Iterate over the table rows to find the file
for row in soup.find_all("tr"):
    columns = row.find_all("td")
    if len(columns) >= 2:
        date_modified = columns[1].text.strip()
        if date_modified.startswith(target_timestamp):
            file_name = columns[0].find("a")["href"]
            break

# Step 2: Download the file
file_url = url + file_name
response = requests.get(file_url)
file_path = file_name.split("/")[-1]

with open(file_path, "wb") as f:
    f.write(response.content)

# Step 3: Load file into Pandas and filter records by timestamp
df = pd.read_csv(file_path)

# Filter records by timestamp
target_time = "14:03"
filtered_df = df[df["Time"] == target_time]

# Get the highest temperature among the filtered records
max_temp = filtered_df["HourlyDryBulbTemperature"].max()

# Print all temperatures at the given timestamp
print(filtered_df)

# Print the highest temperature
print("Highest Temperature:", max_temp)
