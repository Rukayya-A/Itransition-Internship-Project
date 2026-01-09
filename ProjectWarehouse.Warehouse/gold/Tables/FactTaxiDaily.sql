CREATE TABLE [gold].[FactTaxiDaily] (

	[date_key] int NOT NULL, 
	[zone_key] int NOT NULL, 
	[total_trips] int NULL, 
	[total_fare] decimal(10,2) NULL, 
	[avg_fare] decimal(10,2) NULL, 
	[avg_trip_duration] decimal(10,2) NULL
);