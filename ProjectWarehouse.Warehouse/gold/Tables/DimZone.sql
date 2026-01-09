CREATE TABLE [gold].[DimZone] (

	[zone_key] int NOT NULL, 
	[location_id] int NULL, 
	[borough] varchar(50) NULL, 
	[zone_name] varchar(100) NULL, 
	[service_zone] varchar(50) NULL
);