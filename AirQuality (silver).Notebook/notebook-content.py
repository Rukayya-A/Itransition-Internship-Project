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

from pyspark.sql.types import *
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Sensor Locations**

# CELL ********************

location_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("country", StructType([
        StructField("id", StringType(), True),
        StructField("code", StringType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("sensors", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("parameter", StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("units", StringType(), True),
                StructField("displayName", StringType(), True)
            ]), True)
        ])
    ), True),
    StructField("isMobile", BooleanType(), True),
    StructField("isMonitor", BooleanType(), True),
    StructField("coordinates", StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ]), True),
    StructField("datetimeFirst", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True),
    StructField("datetimeLast", StructType([
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True)
    ]), True)
])

locations = spark.read.text("Files/bronze/airquality/locations.json")

locations_clean = (
    locations
    .select(
        regexp_replace(
            regexp_replace(col("value"), '^"|"$', ''),
            '""', '"'
        ).alias("loc_str")
    )
)

locations_parsed = (
    locations_clean
    .select(from_json(col("loc_str"), location_schema).alias("loc_data"))
    .filter(col("loc_data").isNotNull())
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations_columns = (
    locations_parsed
    .withColumn("sensor", explode(col("loc_data.sensors")))
    .select(
        col("loc_data.id").alias("location_id"),
        col("loc_data.name").alias("location_name"),
        col("loc_data.locality"),
        col("loc_data.timezone"),
        col("loc_data.country.id").alias("country_id"),
        col("loc_data.country.code").alias("country_code"),
        col("loc_data.country.name").alias("country_name"),

        col("sensor.id").alias("sensor_id"),
        col("sensor.name").alias("sensor_name"),
        col("sensor.parameter.id").alias("parameter_id"),
        col("sensor.parameter.name").alias("parameter_name"),
        col("sensor.parameter.units").alias("parameter_units"),
        col("sensor.parameter.displayName").alias("parameter_display_name"),

        col("loc_data.isMobile"),
        col("loc_data.isMonitor"),
        col("loc_data.coordinates.latitude"),
        col("loc_data.coordinates.longitude"),
        to_timestamp("loc_data.datetimeFirst.utc").alias("datetime_first_utc"),
        to_timestamp("loc_data.datetimeFirst.local").alias("datetime_first_local"),
        to_timestamp("loc_data.datetimeLast.utc").alias("datetime_last_utc"),
        to_timestamp("loc_data.datetimeLast.local").alias("datetime_last_local")
    )
)

locations_dedup = (
    locations_columns
    .filter(col("parameter_name").isin(["co","o3","no2","pm25"]))
    .dropDuplicates(["sensor_id"])
)
locations_silver = (
    locations_dedup
    .withColumn("source_system", lit("OPEN_AQ")) 
    .withColumn("ingestion_date", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

locations_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.airquality_locations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Measurements**

# CELL ********************

measurement_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("measurement", StructType([
        StructField("value", DoubleType(), True),
        StructField("parameter", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("units", StringType(), True),
        ]), True),
        StructField("period", StructType([
            StructField("datetimeFrom", StructType([
                StructField("utc", StringType(), True),
                StructField("local", StringType(), True)
            ]), True),
            StructField("datetimeTo", StructType([
                StructField("utc", StringType(), True),
                StructField("local", StringType(), True)
            ]), True),
            StructField("interval", StringType(), True)
        ]), True),
    ]), True)
])


measurements = spark.read.text(
    "Files/bronze/airquality/measurements.json"
)

measurements_clean = measurements.select(
    regexp_replace(
        regexp_replace(col("value"), '^"|"$', ''),
        '""', '"'
    ).alias("msr_str")
)

measurements_parsed = (
    measurements_clean
    .select(from_json(col("msr_str"), measurement_schema).alias("msr_data"))
    .filter(col("msr_data").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

measurements_columns = measurements_parsed.select(
    col("msr_data.sensor_id").cast("string").alias("sensor_id"),

    col("msr_data.measurement.value").cast("double").alias("value"),

    col("msr_data.measurement.parameter.id").cast("string").alias("parameter_id"),
    col("msr_data.measurement.parameter.name").cast("string").alias("parameter_name"),
    col("msr_data.measurement.parameter.units").cast("string").alias("parameter_units"),

    to_timestamp("msr_data.measurement.period.datetimeFrom.utc").alias("datetime_from_utc"),
    to_timestamp("msr_data.measurement.period.datetimeFrom.local").alias("datetime_from_local"),

    to_timestamp("msr_data.measurement.period.datetimeTo.utc").alias("datetime_to_utc"),
    to_timestamp("msr_data.measurement.period.datetimeTo.local").alias("datetime_to_local"),

    col("msr_data.measurement.period.interval").cast("string").alias("interval")
)

measurements_dedup = (
    measurements_columns
    .filter(
        (col("datetime_from_local") >= "2021-01-01") &
        (col("datetime_from_local") <  "2026-01-01")
    )
    .dropDuplicates(["sensor_id", "parameter_id", "datetime_from_local"])
)

measurements_silver = (
    measurements_dedup
    .withColumn("date", to_date(col("datetime_from_local")))
    .withColumn("year", year(col("datetime_from_local")))
    .withColumn("hour", hour(col("datetime_from_local")))
    .withColumn("weekday", dayofweek(col("datetime_from_local")))
    .withColumn("source_system", lit("OPEN_AQ")) 
    .withColumn("ingestion_date", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

measurements_silver.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("year") \
    .saveAsTable("silver.airquality_measurements")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
