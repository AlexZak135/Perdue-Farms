# Title: Perdue Farms Analysis
# Author: Alexander Zakrzeski
# Date: July 13, 2025

# Part 1: Setup and Configuration

# Load to import, clean, and wrangle data
import os 
import polars as pl

# Load to produce data visualizations
from mizani.formatters import label_comma, label_dollar, percent_format
from plotnine import *

# Define a function to generate summary statistics for lateness
def late_ss(df, column, text, number):
    # Select columns, group by a column, and calculate summary statistics
    df = (
        df.select(column, "late") 
          .group_by(column)
          .agg((pl.col("late") == "Yes").mean().round(2).alias("prop_late"),
               pl.len().alias("deliveries"),
               pl.lit(f"Top 10 {text} by % Late Deliveries").alias("statistic"))
          # Rename a column, filter, drop a column, and sort rows
          .rename({column: "id"})
          .filter(pl.col("deliveries") >= number)
          .drop("deliveries")          
          .sort("prop_late", descending = True)
          # Keep the first 10 rows
          .limit(10)
        )
                      
    # Return the dataframe
    return df

# Define a function to generate summary statistics for held time 
def held_time_ss(df, summary_statistic):
    # Select columns, create a new column, and group by a column
    df = (
        df.select("dropoff_id", "minutes_held")
          .with_columns(
              (pl.col("minutes_held") / 60).alias("held_time")
              ) 
          .group_by("dropoff_id")
        )
            
    # Calculate summary statistics
    if summary_statistic == "mean":
        df = df.agg(pl.col("held_time").mean().round().alias("held_time"), 
                    pl.lit("Top 10 Customers by Avg. Held Time") 
                      .alias("statistic"))
    elif summary_statistic == "sum":
        df = df.agg(pl.col("held_time").sum().round().alias("held_time"), 
                    pl.lit("Top 10 Customers by Total Held Time") 
                      .alias("statistic"))
                      
    # Sort rows and keep the first 10 rows
    df = df.sort("held_time", descending = True).limit(10)
    
    # Return the dataframe
    return df

# Define a function to generate summary statistics for savings
def dollar_savings_ss(df, summary_statistic):
    # Select columns, create a new column, and group by a column
    df = (
        df.select("dropoff_id", "minutes_held") 
          .with_columns(
              (pl.col("minutes_held") * (65 / 60) - 25).alias("dollar_savings")
              ) 
          .group_by("dropoff_id")
        )
    
    # Calculate summary statistics
    if summary_statistic == "mean":
        df = df.agg(pl.col("dollar_savings").mean().round() 
                      .alias("dollar_savings"), 
                    pl.lit("Top 10 Customers by Avg. Savings") 
                      .alias("statistic"))
    elif summary_statistic == "sum": 
        df = df.agg(pl.col("dollar_savings").sum().round()
                      .alias("dollar_savings"),
                    pl.lit("Top 10 Customers by Total Savings") 
                      .alias("statistic"))
                           
    # Sort rows and keep the first 10 rows
    df = df.sort("dollar_savings", descending = True).limit(10)
    
    # Return the dataframe
    return df
        
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
perdue_farms = (
    tms.rename({"driver_#": "driver_number", 
                "#_of_stops": "number_of_stops"})
       .filter((pl.col("carrier_name") == "Perdue") & 
               (pl.col("number_of_stops") == 1))
       .with_columns(
           *[pl.col(c).cast(pl.Utf8).str.zfill(6).alias(c) 
             if c == "driver_number" else pl.col(c).cast(pl.Utf8).alias(c) 
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
late = pl.concat([late_ss(perdue_farms, "driver_number", "Drivers", 10), 
                  late_ss(perdue_farms, "dropoff_id", "Customers", 15)], 
                 how = "vertical")
held_time = pl.concat([perdue_farms.pipe(held_time_ss, "mean"), 
                       perdue_farms.pipe(held_time_ss, "sum")], 
                      how = "vertical")
savings = pl.concat([perdue_farms.pipe(dollar_savings_ss, "mean"), 
                     perdue_farms.pipe(dollar_savings_ss, "sum")], 
                    how = "vertical")

# Part 3: Data Visualization

# Create a faceted bar chart to display summary statistics for lateness
(ggplot(late, aes(x = "reorder(id, prop_late)", y = "prop_late")) +
   geom_col(width = 0.825, fill = "#005288") +
   scale_y_continuous(labels = percent_format()) +
   labs(title = "Figure 1: Summary Statistics for Late Deliveries", 
        x = "ID", y = "Percentage") +
   facet_wrap(facets = "statistic", ncol = 2, scales = "free") +
   coord_flip() +
   theme_538() + 
   theme(panel_grid_major_y = element_blank()))

# Create a faceted bar chart to display summary statistics for held time 
(ggplot(held_time, aes(x = "reorder(dropoff_id, held_time)", y = "held_time")) +
   geom_col(width = 0.825, fill = "#005288") +
   scale_y_continuous(labels = label_comma()) +
   labs(title = "Figure 2: Summary Statistics for Customer Held Time", 
        x = "Customer ID", y = "Hours") +
   facet_wrap(facets = "statistic", ncol = 2, scales = "free") +
   coord_flip() +
   theme_538() + 
   theme(panel_grid_major_y = element_blank()))

# Create a faceted bar chart to display summary statistics for savings
(ggplot(savings, aes(x = "reorder(dropoff_id, dollar_savings)", 
                     y = "dollar_savings")) +
   geom_col(width = 0.825, fill = "#005288") +
   scale_y_continuous(labels = label_dollar(accuracy = 1, big_mark = ",")) +
   labs(title = "Figure 3: Summary Statistics for Savings from Drop Trailers", 
        x = "Customer ID", y = "Dollars") +
   facet_wrap("statistic", ncol = 2, scales = "free") + 
   coord_flip() +
   theme_538() +
   theme(panel_grid_major_y = element_blank()))