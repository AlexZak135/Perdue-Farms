# Title: Perdue Farms Analysis
# Author: Alexander Zakrzeski
# Date: June 15, 2025

# Part 1: Setup and Configuration

# Load to import, clean, and wrangle data
import os 
import polars as pl

# Set working directory
os.chdir("/Users/atz5/Desktop/Perdue-Farms/Data") 

# Part 2: Data Preprocessing

# Load the data from the parquet files
tms, dc, otht = (
    [pl.read_parquet(file).rename(lambda col: col.lower().replace(" ", "_")) 
     for file in ["TMS-Data.parquet", "Delivery-Cost-Data.parquet", 
                  "On-Time-Held-Time-Data.parquet"]]
    )

tms = tms.with_columns(pl.col("shipment_number").cast(pl.Utf8))
tms = tms.filter(pl.col("carrier_name") == "Perdue")
tms = tms.drop("carrier_name")



check = tms.select(pl.col("carrier_name").value_counts(sort = True))