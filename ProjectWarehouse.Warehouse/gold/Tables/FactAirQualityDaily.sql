CREATE TABLE [gold].[FactAirQualityDaily] (

	[measurement_key] bigint IDENTITY NOT NULL, 
	[date_key] bigint NOT NULL, 
	[sensor_key] bigint NOT NULL, 
	[location_id] int NOT NULL, 
	[total_measurements] int NULL, 
	[avg_value] float NULL, 
	[min_value] float NULL, 
	[max_value] float NULL, 
	[parameter_name] varchar(50) NULL, 
	[parameter_units] varchar(20) NULL
);