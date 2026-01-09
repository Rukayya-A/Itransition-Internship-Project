# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11607498-1180-4567-98e7-39430f64772b",
# META       "default_lakehouse_name": "ProjectLakehouse",
# META       "default_lakehouse_workspace_id": "6d40a6b8-5032-4131-96e6-6d99b8654d16",
# META       "known_lakehouses": [
# META         {
# META           "id": "11607498-1180-4567-98e7-39430f64772b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import *

mobility = spark.table("silver.mobility_yellow_taxi_trips")
airquality = spark.table("silver.airquality_measurements")
fx = spark.table("silver.economy_fx")
gdp = spark.table("silver.economy_gdp")

mob_dates = mobility.select(col("pickup_datetime").alias("date"))
aq_dates = airquality.select(col("datetime_from_local").alias("date"))
fx_dates = fx.select(col("time_period").alias("date"))
gdp_dates = gdp.select(expr("make_date(year, 1, 1)").alias("date"))

all_dates = mob_dates.union(aq_dates).union(fx_dates).union(gdp_dates)
date_bounds = all_dates.agg(min("date").alias("min_date"), max("date").alias("max_date")).collect()[0]

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]


calendar_df = spark.range(0, (max_date - min_date).days + 1) \
    .withColumn("date", date_add(lit(min_date), col("id").cast("int"))) \
    .drop("id")

calendar_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.calendar_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
