CREATE TABLE [gold].[FactZoneMeasurementsDaily] (

	[zone_measurement_key] bigint IDENTITY NOT NULL, 
	[date_key] bigint NOT NULL, 
	[zone_key] bigint NOT NULL, 
	[total_trips] int NULL, 
	[total_passengers] int NULL, 
	[avg_passengers] float NULL, 
	[total_base_fare_usd] float NULL, 
	[total_trip_amount_usd] float NULL, 
	[avg_base_fare_usd] float NULL, 
	[avg_trip_amount_usd] float NULL, 
	[total_distance_miles] float NULL, 
	[avg_trip_distance_miles] float NULL, 
	[avg_trip_duration_min] float NULL, 
	[total_measurements] int NULL, 
	[avg_pm25] float NULL, 
	[avg_no2] float NULL, 
	[avg_o3] float NULL, 
	[avg_co] float NULL
);