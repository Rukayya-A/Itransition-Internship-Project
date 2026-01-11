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

%pip install geopandas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
import zipfile, io, os
import geopandas as gpd
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Zones Shapefiles**

# CELL ********************

zones_raw = spark.read.format("binaryFile").load("Files/bronze/mobility/zones/taxi_zones_shapefiles.zip")

zip_bytes = zones_raw.collect()[0].content

extract_path = "/tmp/taxi_zones"
os.makedirs(extract_path, exist_ok=True)
with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as z:
    z.extractall(extract_path)

gdf = gpd.read_file(os.path.join(extract_path, "taxi_zones.shp"))

gdf["geometry_geojson"] = gdf["geometry"].apply(
    lambda geom: json.dumps(geom.__geo_interface__)
)

gdf = gdf.drop(columns="geometry")

zones = spark.createDataFrame(gdf)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zones_columns = zones.select(
    col("OBJECTID").cast("int").alias("object_id"),
    col("Shape_Leng").cast("double").alias("shape_length"),
    col("Shape_Area").cast("double").alias("shape_area"),
    lower(trim(col("Zone"))).alias("zone"),
    col("LocationID").cast("int").alias("location_id"),
    lower(trim(col("Borough"))).alias("borough"),
    col("geometry_geojson").cast("string").alias("geometry_geojson")
)

zones_dedup = zones_columns.dropDuplicates(["location_id", "zone", "borough"])

zones_silver = (
    zones_dedup
    .withColumn("source_system", lit("NYC_TLC"))
    .withColumn("ingestion_ts", current_timestamp())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zones_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.mobility_taxi_zones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Yellow Taxis**

# CELL ********************

location_id_map = {
    57: 56,
    104: 103,
    105: 103
}

def map_location_id(column):
    expr = col(column)
    for orig, canon in location_id_map.items():
        expr = when(col(column) == orig, canon).otherwise(expr)
    return expr

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
                col("trip_distance").cast("double").alias("trip_distance_miles"),
                col("PULocationID").cast("int").alias("pickup_location_id"),
                col("DOLocationID").cast("int").alias("dropoff_location_id"),
                col("passenger_count").cast("int"),
                col("fare_amount").cast("double").alias("fare_amount_usd"),
                col("total_amount").cast("double").alias("total_amount_usd"),
            )

            mobility_dedup = (
                mobility_columns
                .filter(
                    (col("pickup_datetime") >= pickup_start) &
                    (col("pickup_datetime") <  pickup_end)
                )
                .filter(~col("pickup_location_id").isin([264, 265]))   # NOT IN NYC according to the lookup table
                .filter("trip_distance_miles > 0")
                .withColumn("pickup_location_id", map_location_id("pickup_location_id"))
                .withColumn("dropoff_location_id", map_location_id("dropoff_location_id"))
                .dropDuplicates(["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id"])
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
