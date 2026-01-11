CREATE TABLE [gold].[DimSensor] (

	[sensor_key] bigint IDENTITY NOT NULL, 
	[zone_key] bigint NULL, 
	[sensor_id] int NOT NULL, 
	[sensor_name] varchar(50) NULL, 
	[parameter_id] int NULL, 
	[parameter_name] varchar(50) NULL, 
	[parameter_units] varchar(50) NULL, 
	[parameter_display_name] varchar(100) NULL, 
	[location_id] int NOT NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[country_code] char(2) NULL, 
	[country_name] varchar(50) NULL
);