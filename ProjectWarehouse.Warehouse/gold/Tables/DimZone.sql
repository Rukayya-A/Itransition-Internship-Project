CREATE TABLE [gold].[DimZone] (

	[zone_key] bigint IDENTITY NOT NULL, 
	[location_id] int NOT NULL, 
	[zone] varchar(100) NULL, 
	[borough] varchar(50) NULL, 
	[geometry_geojson] varchar(max) NULL
);