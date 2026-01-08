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
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

years = ["2021", "2022", "2023", "2024", "2025"]
months = [f"{m:02d}" for m in range(1, 13)]

pickup_start = "2021-01-01"
pickup_end   = "2026-01-01"

for y in years:
    for m in months:
        print(f"Processing {y}-{m}...")

        path = f"Files/bronze/mobility/yellow_taxi/{y}/yellow_tripdata_{y}-{m}.parquet"
        try:
            mobility = spark.read.parquet(path)

            mobility_columns = mobility.select(
                to_timestamp("tpep_pickup_datetime").alias("pickup_datetime"),
                to_timestamp("tpep_dropoff_datetime").alias("dropoff_datetime"),
                col("trip_distance").cast("double").alias("trip_distance"),
                col("PULocationID").cast("int").alias("pickup_location_id"),
                col("DOLocationID").cast("int").alias("dropoff_location_id"),
                col("fare_amount").cast("double").alias("fare_amount"),
                col("total_amount").cast("double").alias("total_amount"),
            )

            mobility_dedup = (
                mobility_columns
                .dropDuplicates(["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id"])
                .filter(
                    (col("pickup_datetime") >= pickup_start) &
                    (col("pickup_datetime") <  pickup_end)
                )
                .filter("trip_distance > 0")
            )

            mobility_silver = (
                mobility_dedup
                .withColumn("trip_duration_min", (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60)
                .withColumn("pickup_year", year("pickup_datetime"))
                .withColumn("pickup_date", to_date("pickup_datetime"))
                .withColumn("pickup_weekday", dayofweek("pickup_datetime"))
                .withColumn("pickup_hour", hour("pickup_datetime"))
                .withColumn("source_system", lit("NYC_TLC"))
                .withColumn("ingestion_ts", current_timestamp())
            )

            mobility_silver.write.format("delta") \
                .mode("append") \
                .partitionBy("pickup_year") \
                .saveAsTable("silver.mobility_yellow_taxi_trips")

            print(f"{y}-{m} processed successfully!")

        except Exception as e:
            print(f"{y}-{m} FAILED: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lookup_schema = StructType([
    StructField("LocationID", IntegerType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

lookup_raw = spark.read \
    .option("header", True) \
    .schema(lookup_schema) \
    .csv("Files/bronze/mobility/taxi_zone_lookup.csv")

lookup_silver = (
    lookup_raw
    .withColumn("borough", lower(trim(col("Borough"))))
    .withColumn("zone", lower(trim(col("Zone"))))
    .withColumn("service_zone", lower(trim(col("service_zone"))))
    .withColumnRenamed("LocationID", "location_id")
    .dropDuplicates()
)

lookup_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.mobility_zone_lookup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
