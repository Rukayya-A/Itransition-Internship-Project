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

%pip install shapely
%pip install pyproj

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
import pandas as pd
import json
from pyproj import Transformer
from shapely.geometry import shape, Point, Polygon, MultiPolygon

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Calendar Dates**

# CELL ********************

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
    .saveAsTable("silver.helper_calendar_dates")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Zone Map**

# CELL ********************

taxi_zones = spark.read.format("delta").table("silver.mobility_taxi_zones").toPandas()
airquality = spark.read.format("delta").table("silver.airquality_locations").toPandas()

def safe_parse_geojson(g):
    if g and isinstance(g, str):
        geom = shape(json.loads(g))
        return geom

taxi_zones["geometry"] = taxi_zones["geometry_geojson"].apply(safe_parse_geojson)

transformer = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)

def project_point(lon, lat):
    x, y = transformer.transform(lon, lat)
    return Point(x, y)

airquality["point"] = airquality.apply(
    lambda row: project_point(row["longitude"], row["latitude"]), axis=1
)

results = []
for _, aq in airquality.iterrows():
    pt = aq["point"]
    for _, tz in taxi_zones.iterrows():
        geom = tz["geometry"]
        if geom is None:
            continue
        if isinstance(geom, Polygon):
            if geom.contains(pt):
                results.append({
                    "airquality_location_id": aq["location_id"],
                    "taxi_location_id": tz["location_id"],
                    "match_type": "inside"
                })
        elif isinstance(geom, MultiPolygon):
            for poly in geom.geoms:
                if poly.contains(pt):
                    results.append({
                        "airquality_location_id": aq["location_id"],
                        "taxi_location_id": tz["location_id"],
                        "match_type": "inside"
                    })
                    break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

map_df = pd.DataFrame(results)
map_df = map_df.drop_duplicates()
spark.createDataFrame(map_df).write.format("delta").mode("overwrite").saveAsTable("silver.helper_zone_map")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import json
from shapely.geometry import shape, Point
from pyproj import Transformer

# -------------------------------------------------------
# 1. Load data from Delta
# -------------------------------------------------------

taxi_zones = (
    spark.read
    .format("delta")
    .table("silver.mobility_taxi_zones")
    .toPandas()
)

airquality = (
    spark.read
    .format("delta")
    .table("silver.airquality_locations")
    .toPandas()
)

# -------------------------------------------------------
# 2. Parse taxi zone geometries (GeoJSON -> Shapely)
#    + fix invalid geometries
# -------------------------------------------------------

def safe_parse_geojson(g):
    if g and isinstance(g, str):
        geom = shape(json.loads(g))
        # Fix invalid polygons (very common for NYC zones)
        if not geom.is_valid:
            geom = geom.buffer(0)
        return geom
    return None

taxi_zones["geometry"] = taxi_zones["geometry_geojson"].apply(safe_parse_geojson)

# -------------------------------------------------------
# 3. Project AQ points from EPSG:4326 -> EPSG:2263
# -------------------------------------------------------

transformer = Transformer.from_crs(
    "EPSG:4326",
    "EPSG:2263",
    always_xy=True
)

def project_point(lon, lat):
    x, y = transformer.transform(lon, lat)
    return Point(x, y)

airquality["point"] = airquality.apply(
    lambda r: project_point(r["longitude"], r["latitude"]),
    axis=1
)

# -------------------------------------------------------
# 4. Spatial join (boundary-safe)
# -------------------------------------------------------

results = []

for _, aq in airquality.iterrows():
    pt = aq["point"]

    for _, tz in taxi_zones.iterrows():
        geom = tz["geometry"]
        if geom is None:
            continue

        # covers() matches interior + boundary points
        if geom.covers(pt):
            results.append({
                "airquality_location_id": aq["location_id"],
                "taxi_location_id": tz["location_id"],
                "match_type": "inside_or_boundary"
            })
            break  # one taxi zone per AQ location

# -------------------------------------------------------
# 5. Results as DataFrame
# -------------------------------------------------------

matches_df = pd.DataFrame(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from shapely.geometry import shape, Point, Polygon, MultiPolygon
from pyproj import Transformer
import pandas as pd

# Load tables into pandas
taxi_zones = spark.read.format("delta").table("silver.mobility_taxi_zones").toPandas()
airquality = spark.read.format("delta").table("silver.airquality_locations").toPandas()

# Parse GeoJSON safely
def safe_parse_geojson(g):
    if g and isinstance(g, str):
        try:
            geom = shape(json.loads(g))
            return geom
        except Exception:
            return None
    return None

taxi_zones["geometry"] = taxi_zones["geometry_geojson"].apply(safe_parse_geojson)

# Transformer: WGS84 → EPSG:2263
transformer = Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)

def project_point(lon, lat):
    x, y = transformer.transform(lon, lat)
    return Point(x, y)

# Project AQ points into EPSG:2263
airquality["point"] = airquality.apply(
    lambda row: project_point(row["longitude"], row["latitude"]), axis=1
)

# Match AQ points to taxi zones
results = []
for _, aq in airquality.iterrows():
    pt = aq["point"]
    for _, tz in taxi_zones.iterrows():
        geom = tz["geometry"]
        if geom is None:
            continue
        if isinstance(geom, Polygon):
            if geom.intersects(pt):   # FIX: use intersects instead of contains
                results.append({
                    "airquality_location_id": aq["location_id"],
                    "taxi_location_id": tz["location_id"],
                    "match_type": "inside_or_boundary"
                })
        elif isinstance(geom, MultiPolygon):
            for poly in geom.geoms:
                if poly.intersects(pt):   # FIX: use intersects
                    results.append({
                        "airquality_location_id": aq["location_id"],
                        "taxi_location_id": tz["location_id"],
                        "match_type": "inside_or_boundary"
                    })
                    break

# Convert results to DataFrame if needed
matches_df = pd.DataFrame(results)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

unmatched = set(airquality["location_id"]) - set(matches_df["airquality_location_id"])
len(unmatched)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
