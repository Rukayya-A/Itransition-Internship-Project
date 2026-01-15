# Unified Mobility–Environment–Economy Analytics Platform (Microsoft Fabric)

This document describes the **actual implemented architecture** of the project as built in Microsoft Fabric. It reflects the real ingestion methods, storage layout, transformations, and data models currently in use.

---

## 1. Architecture Overview

* **Single Microsoft Fabric workspace** (all artifacts co-located)
* Purpose: end-to-end analytics demonstration rather than environment separation
* Design follows **Medallion Architecture**:

  * **Bronze**: raw, immutable ingestion (files)
  * **Silver**: cleansed, standardized Delta tables
  * **Gold**: dimensional model in Fabric Warehouse

Core Fabric components used:

* OneLake Lakehouse (`ProjectLakehouse`)
* Data Factory Pipelines (file copy)
* Dataflow Gen2 (API & CSV ingestion)
* Fabric Notebooks (PySpark transformations)
* Fabric Warehouse (star schema)

---

## 2. Bronze Layer – Raw Data Ingestion

### 2.1 Design Principles

* Raw data is landed **as-is** with no business logic
* Append-only, immutable storage
* Schema-on-read
* Bronze is **not queried directly** for analytics

### 2.2 Storage Layout (OneLake)

All Bronze data is stored under:

```
ProjectLakehouse/Files/bronze/
```

#### NYC Taxi – Mobility (Parquet)

* Ingestion method: **Data Factory Pipeline – Copy Activity**
* Source: NYC TLC monthly Parquet files
* Files are stored **raw and unchanged**
* Folder partitioning:

```
Files/bronze/mobility/yellow_taxi/
  └── year=YYYY/
```

* Each month is ingested independently
* Historical files are retained indefinitely

#### OpenAQ – Air Quality (API JSON)

* Ingestion method: **Dataflow Gen2**
* API responses are landed in file form
* Pagination handled in Dataflow
* Storage path:

```
Files/bronze/airquality/files/
```

#### Economy – GDP & FX

* Ingestion method: **Dataflow Gen2**
* World Bank GDP: JSON API
* ECB FX rates: CSV endpoint
* Storage path:

```
Files/bronze/economy/files/
```

---

## 3. Silver Layer – Cleansed & Conformed Data

### 3.1 Transformation Strategy

* All Silver transformations are implemented in **Fabric Notebooks (PySpark)**
* Logic includes schema standardization, normalization, enrichment, and helper tables
* Output is written as **Delta tables** to the Lakehouse Tables area

### 3.2 Silver Tables

#### Mobility

**silver.mobility_yellow_taxi_trips**

* Grain: one row per taxi trip
* Derived from Bronze Parquet files
* Used as the atomic mobility dataset

**silver.mobility_taxi_zones**

* Reference table for NYC taxi zones
* Includes zone metadata and geometries

#### Air Quality

**silver.airquality_locations**

* Unique air quality monitoring locations
* Includes coordinates and identifiers

**silver.airquality_measurements**

* Grain: one sensor × timestamp × pollutant
* Cleaned and standardized measurement values

**silver.helper_airquality_zones**

* Mapping table between air quality locations/sensors and taxi zones
* Enables cross-domain spatial analysis

#### Economy

**silver.economy_ecb_fx**

* Daily USD/EUR exchange rates

**silver.economy_wb_gdp**

* Yearly GDP values per country

#### Shared Helpers

**silver.helper_calendar_dates**

* Calendar date dimension generated centrally
* Used to populate Gold `DimDate`

---

## 4. Gold Layer – Dimensional Model (Fabric Warehouse)

The Gold layer resides in the **Fabric Warehouse** and serves as the **single source of truth for reporting and analytics**.

### 4.1 Dimension Tables

#### gold.DimDate

* Date surrogate: `date_key` (YYYYMMDD)
* Calendar attributes: year, quarter, month, weekday, weekend flag

#### gold.DimZone

* NYC taxi zones
* `zone_location_id` corresponds to TLC location ID
* Geometry stored for reference and lineage (not used in aggregations)

#### gold.DimSensor

* Air quality sensor dimension
* One row per sensor and pollutant context
* Includes coordinates and zone mapping

#### gold.DimFX

* Daily FX rates
* EUR → USD conversion
* Linked to calendar via `date_key`

#### gold.DimGDP

* Yearly GDP by country
* Macro-economic context dimension

---

### 4.2 Fact Tables

#### gold.FactTaxiDaily

* Grain: **date × pickup zone**
* Derived from `silver.mobility_yellow_taxi_trips`
* Measures:

  * total trips and passengers
  * total and average fares
  * distance and duration metrics

#### gold.FactAirQualityDaily

* Grain: **date × sensor × pollutant**
* Aggregated daily statistics (avg/min/max)
* Linked to zones via sensor mapping

#### gold.FactZoneMeasurementsDaily

* **Integrated cross-domain fact table**
* Grain: **date × zone**
* Combines:

  * mobility metrics (taxi activity)
  * air quality metrics (PM2.5, NO2, O3, CO)
* Enables direct correlation analysis without complex joins

---

## 5. Orchestration & Refresh

* Bronze ingestion triggered per dataset (monthly or daily)
* Silver notebooks executed after Bronze availability
* Gold tables refreshed from Silver outputs
* Refresh schedules aligned to data source latency:

  * Taxi: monthly
  * Air Quality: daily
  * FX: daily
  * GDP: yearly

---

## 6. Governance & Data Management

### Data Quality

* Validation applied in Silver layer
* Null checks on keys
* Removal of invalid measurements

### Security

* Workspace-level access control
* Warehouse used as controlled reporting layer

### Lineage

* Logical lineage documented from source → Bronze → Silver → Gold
* Supports Fabric lineage visualization

---

## 7. Assumptions & Limitations

* Air quality sensors are spatially mapped to taxi zones (approximation)
* Taxi analysis focuses on pickup zones only
* Economic indicators provide macro-level context, not city-level causation
* Correlation does not imply causation

---

## 8. Outcome

This project delivers a **unified analytics platform** that demonstrates how urban mobility, environmental quality, and economic context can be analyzed together using Microsoft Fabric’s full data stack. The architecture is reproducible, extensible, and suitable for portfolio, interview, and instructional use.

