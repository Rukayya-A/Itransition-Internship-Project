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

# ## **GDP**

# CELL ********************

gdp_schema = StructType([
    StructField("indicator", StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("country", StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("countryiso3code", StringType(), True),
    StructField("date", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("obs_status", StringType(), True),
    StructField("decimal", IntegerType(), True)
])

gdp_raw = spark.read.text("Files/bronze/economy/wb_gdp.JSON")

gdp_clean = (
    gdp_raw
    .select(
        regexp_replace(
            regexp_replace(col("value"), '^"|"$', ''),
            '""', '"'
        ).alias("gdp_str")
    )
)

gdp_parsed = (
    gdp_clean
    .select(from_json(col("gdp_str"), gdp_schema).alias("gdp_data"))
    .filter(col("gdp_data").isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


gdp_columns = gdp_parsed.select(
    col("gdp_data.country.id").alias("country_id"),
    col("gdp_data.country.value").alias("country_name"),
    col("gdp_data.countryiso3code").alias("iso3"),
    col("gdp_data.indicator.id").alias("indicator_id"),
    col("gdp_data.indicator.value").alias("indicator_name"),
    col("gdp_data.date").cast("int").alias("year"),
    col("gdp_data.value").alias("gdp_usd")
)

gdp_silver = (
    gdp_columns
    .filter(
        (col("year") >= 2021) &
        (col("year") < 2026)
    )
    .withColumn("source_system", lit("WB_GDP")) 
    .withColumn("ingestion_ts", current_timestamp())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gdp_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.economy_gdp")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## **Exchange Rate**

# CELL ********************

fx_raw = spark.read.option("header", True).csv("Files/bronze/economy/ecb_fx.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx_silver = (
    fx_raw
    .select(
        col("KEY").cast("string").alias("key"),
        col("CURRENCY").cast("string").alias("currency"),
        col("CURRENCY_DENOM").cast("string").alias("currency_denom"),
        to_date(col("TIME_PERIOD")).alias("time_period"),
        col("OBS_VALUE").cast("double").alias("obs_value"),
        col("OBS_STATUS").cast("string").alias("obs_status"),
        col("DECIMALS").cast("int").alias("decimals"),
        col("TITLE").cast("string").alias("title"),
        lit("ECB_FX").alias("source_system"),
        current_timestamp().alias("ingestion_ts")
    )
    .filter(
        (col("time_period") >= "2021-01-01") &
        (col("time_period") < "2026-01-01")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.economy_fx")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
