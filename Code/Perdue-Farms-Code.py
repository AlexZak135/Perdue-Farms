# Title: Perdue Farms Analysis
# Author: Alexander Zakrzeski
# Date: June 30, 2025

# Part 1: Setup and Configuration

# Load to import, clean, and wrangle data
import os 
import polars as pl

# Load to produce data visualizations
from mizani.formatters import label_dollar
import numpy as np
from plotnine import *

# Set working directory
os.chdir("/Users/atz5/Desktop/Perdue-Farms/Data") 

# Part 2: Data Preprocessing

# Load the data from the parquet files
tms, dc, otht = (
    [pl.read_parquet(file).rename(lambda col: col.lower().replace(" ", "_")) 
     for file in ["TMS-Data.parquet", "Delivery-Cost-Data.parquet", 
                  "On-Time-Held-Time-Data.parquet"]]
    )

# Rename columns, filter, modify values in columns, and create a new column
combined = (
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
      # Drop the columns and perform a left join
      ).drop("carrier_name", "pickup_id", "pickup_depart_date", 
             "pickup_depart_time", "number_of_stops")
       .join((
           # Rename columns, modify values in a column, and drop a column
           dc.rename({"gen3,shipment_number": "shipment_number", 
                      "customer": "dropoff_id"})
             .with_columns(
                 pl.col("shipment_number").str.replace(r"^S000", "")
                   .alias("shipment_number")
                 ).drop("gen5,location")
           ), on = ["shipment_number", "dropoff_id"], how = "left")
       # Perform a left join
       .join((
           # Rename columns and filter 
           otht.rename({"load_#": "shipment_number", 
                        "late?_(yes/no)": "late", 
                        "sold_to": "dropoff_id"})
               .filter(pl.col("carrier_name") == "Perdue")
               # Modify values in columns, create new columns, and drop columns
               .with_columns(
                   *[pl.col(c).cast(pl.Utf8).alias(c) 
                     for c in ["shipment_number", "dropoff_id"]],
                   *[(pl.col(f"{p}_date").dt.date().cast(pl.Utf8) + " " +
                      pl.col(f"{p}_time").dt.time().cast(pl.Utf8)).str.strptime(
                          pl.Datetime, "%Y-%m-%d %H:%M:%S"
                          )
                        .alias(f"{p}_timestamp")
                     for p in ["sched_arrive", "actual_arrive", "empty"]]
              ).with_columns(
                   pl.when(pl.col("actual_arrive_timestamp") - 
                           pl.col("sched_arrive_timestamp") > 
                           pl.duration(minutes = 30))
                     .then(pl.lit("Yes"))
                     .otherwise(pl.lit("No"))
                     .alias("late"),
                   ((pl.col("empty_timestamp") - 
                     pl.col("actual_arrive_timestamp")) 
                       .dt.total_seconds() / 60).round(2)
                       .alias("minutes_held")
              ).drop("carrier_name", "sched_arrive_date", "sched_arrive_time", 
                     "actual_arrive_date", "actual_arrive_time", "empty_date", 
                     "empty_time", "held")
           ), on = ["shipment_number", "dropoff_id"], how = "left")
       
       # Drop rows with null values and reorder the columns
       .drop_nulls()
       .select("shipment_number", "driver_number", "pickup_city", 
               "pickup_state", "pickup_timestamp", "dropoff_id", "dropoff_city",
               "dropoff_state", "sched_arrive_timestamp", 
               "actual_arrive_timestamp", "empty_timestamp", "pounds_shipped",
               "direct_load_cost", "late", "minutes_held")
    )





# Concatenate the dataframes vertically
savings = (
    pl.concat([
        # Select columns, calculate means, sort rows, and keep the first 10 rows 
        (combined.select("dropoff_id", "minutes_held") 
                 .with_columns(
                     (pl.col("minutes_held") * (65 / 60) - 25).alias("savings")
                     ) 
                 .group_by("dropoff_id")     
                 .agg(pl.col("savings").mean().round().alias("savings"), 
                      pl.lit("mean").alias("statistic")) 
                 .sort("savings", descending = True) 
                 .limit(10)),
        # Select columns, calculate sums, sort rows, and keep the first 10 rows
        (combined.select("dropoff_id", "minutes_held") 
                 .with_columns(
                     (pl.col("minutes_held") * (65 / 60) - 25).alias("savings")
                     ) 
                 .group_by("dropoff_id") 
                 .agg(pl.col("savings").sum().round().alias("savings"),
                      pl.lit("sum").alias("statistic")) 
                 .sort("savings", descending = True)
                 .limit(10))
        ], how = "vertical")
    )
            
# Part 3: Data Visualization

# Create a bar chart to display summary statistics

(
  ggplot(savings, aes(x = "reorder(dropoff_id, savings)", y = "savings")) +
    geom_col(fill = "#005288") +
    scale_y_continuous(breaks = np.linspace(0, 120000, 4), 
                       labels = label_dollar(accuracy = 1, big_mark = ",")) +
    labs(title = "Figure 1: Top Customers by Savings from Drop Trailer Usage", 
         x = "Customer ID", y = "") +
    coord_flip() +
    theme_538()
  ) 