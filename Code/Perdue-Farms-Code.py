# Title: Perdue Farms Analysis
# Author: Alexander Zakrzeski
# Date: June 17, 2025

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

tms = (
    tms.rename({"driver_#": "driver_number", 
                "#_of_stops": "number_of_stops"})
       .filter((pl.col("carrier_name") == "Perdue") & 
               (pl.col("number_of_stops") == 1))
       .with_columns(
           *[pl.col(c).cast(pl.Utf8).str.zfill(6) if c == "driver_number" 
             else pl.col(c).cast(pl.Utf8).alias(c)
             for c in ["shipment_number", "driver_number"]], 
           pl.col("pickup_city").str.replace(r"^St ", "St. ")
             .alias("pickup_city"),
           pl.col("dropoff_city").str.replace("LINDEN", "Linden")
             .alias("dropoff_city"),
           (pl.col("pickup_depart_date") + " " + 
            pl.col("pickup_depart_time")).str.strptime(pl.Datetime, 
                                                       "%Y-%m-%d %H:%M:%S")
              .alias("pickup_timestamp") 
      ).drop("carrier_name", "pickup_id", "pickup_depart_date", 
             "pickup_depart_time", "number_of_stops")
    )