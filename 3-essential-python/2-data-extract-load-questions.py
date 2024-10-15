# Extract: Process to pull data from Source system
# Load: Process to write data to a destination system

# Common upstream & downstream systems
# OLTP Databases: Postgres, MySQL, sqlite3, etc
# OLAP Databases: Snowflake, BigQuery, Clickhouse, DuckDB, etc
# Cloud data storage: AWS S3, GCP Cloud Store, Minio, etc
# Queue systems: Kafka, Redpanda, etc
# API
# Local disk: csv, excel, json, xml files
# SFTP\FTP server

# Databases: When reading or writing to a database we use a database driver. Database drivers are libraries that we can use to read or write to a database.
# Question: How do you read data from a sqlite3 database and write to a DuckDB database?
# Hint: Look at importing the database libraries for sqlite3 and duckdb and create connections to talk to the respective databases
import sqlite3
import duckdb

sqliteCon = sqlite3.connect('tpch.db')

# Fetch data from the SQLite Customer table
sqlite_customer = sqliteCon.execute("Select * from Customer").fetchall()

# Insert data into the DuckDB Customer table
duckdbCon = duckdb.connect('duckdb.db')
duckdbCon.begin()
duckdbCon.executemany('Insert into Customer VALUES (?,?,?,?,?,?)',sqlite_customer)
# Hint: Look for Commit and close the connections
# Commit tells the DB connection to send the data to the database and commit it, if you don't commit the data will not be inserted
duckdbCon.commit()
# We should close the connection, as DB connections are expensive
sqliteCon.close()
duckdbCon.close()

# Cloud storage
# Question: How do you read data from the S3 location given below and write the data to a DuckDB database?
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import gzip
import csv
from io import StringIO

# Data source: https://docs.opendata.aws/noaa-ghcn-pds/readme.html station data at path "csv.gz/by_station/ASN00002022.csv.gz"
# Hint: Use boto3 client with UNSIGNED config to access the S3 bucket
# Hint: The data will be zipped you have to unzip it and decode it to utf-8
# AWS S3 bucket and file details
bucket_name = "noaa-ghcn-pds"
file_key = "csv.gz/by_station/ASN00002022.csv.gz"
# Create a boto3 client with anonymous access
s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

# Download the CSV file from S3
obj = s3_client.get_object(Bucket = bucket_name, Key = file_key)
compressed_data = obj['Body'].read()

# Decompress the gzip data
csv_data = gzip.decompress(compressed_data).decode('utf-8')

# Read the CSV file using csv.reader
csvreader = csv.reader(StringIO(csv_data))
insertData = list(csvreader)

# Connect to the DuckDB database (assume WeatherData table exists)
duckdbCon = duckdb.connect('duckdb.db')

# Insert data into the DuckDB WeatherData table
duckdbCon.begin()

# insert statment here
duckdbCon.executemany("Insert into WeatherData Values (?,?,?,?,?,?,?,?)", insertData)

duckdbCon.commit()
# duckdbCon.rollback()
duckdbCon.close()

# API
# Question: How do you read data from the CoinCap API given below and write the data to a DuckDB database?
# URL: "https://api.coincap.io/v2/exchanges"
# Hint: use requests library

# Define the API endpoint
url = "https://api.coincap.io/v2/exchanges"

# Fetch data from the CoinCap API
import requests

apiResponse = requests.get("https://api.coincap.io/v2/exchanges")
jsonData = apiResponse.json()['data']
# Prepare data for insertion
# Hint: Ensure that the data types of the data to be inserted is compatible with DuckDBs data column types in ./setup_db.py
dataToInsert = [
    (
    str(exchange['exchangeId']),
    str(exchange['name']),
    int(exchange['rank']),
    float(exchange['percentTotalVolume']) if exchange['percentTotalVolume'] else None,
    float(exchange['volumeUsd']) if exchange['volumeUsd'] else None,
    str(exchange['tradingPairs']),
    bool(exchange['socket']),
    str(exchange['exchangeUrl']),
    int(exchange['updated'])
)
for exchange in jsonData
]

# Connect to the DuckDB database
duckdbCon = duckdb.connect('duckdb.db')
# Insert data into the DuckDB Exchanges table
duckdbCon.executemany("Insert Into Exchanges Values (?,?,?,?,?,?,?,?,?)",dataToInsert)
duckdbCon.commit()
duckdbCon.close()

# Local disk
# Question: How do you read a CSV file from local disk and write it to a database?
# Look up open function with csvreader for python
with open('./data/customers.csv','r', newline="")as file:
  csvFile = csv.reader(file)
  next(csvFile)  # Skip header row
  for lines in csvFile:
        print(lines)

# Web scraping
# Questions: Use beatiful soup to scrape the below website and print all the links in that website
# URL of the website to scrape
url = 'https://example.com'

# import necessary libraries 
from bs4 import BeautifulSoup 
import requests 
import re 
        
# request for HTML document of given url 
response = requests.get(url) 

# create document 
html_document = response.text 
  
# create soap object 
soup = BeautifulSoup(html_document, 'html.parser') 
  
# find all the anchor tags with "href"  
# attribute starting with "https://" 
for link in soup.find_all('a'
                          #, attrs={'href': re.compile("^https://")}
                          ): 
    # display the actual urls 
    print(link.get('href'))   