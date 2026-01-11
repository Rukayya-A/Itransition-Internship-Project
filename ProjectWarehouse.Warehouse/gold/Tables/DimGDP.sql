CREATE TABLE [gold].[DimGDP] (

	[gdp_key] bigint IDENTITY NOT NULL, 
	[country_id] char(2) NOT NULL, 
	[country_name] varchar(50) NOT NULL, 
	[iso3] char(3) NOT NULL, 
	[year] int NOT NULL, 
	[gdp_usd] float NOT NULL
);