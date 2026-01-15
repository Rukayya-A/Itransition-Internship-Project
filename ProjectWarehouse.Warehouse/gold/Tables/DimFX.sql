CREATE TABLE [gold].[DimFX] (

	[fx_key] bigint IDENTITY NOT NULL, 
	[currency] varchar(10) NOT NULL, 
	[currency_denom] varchar(10) NOT NULL, 
	[time_period] date NOT NULL, 
	[eur_to_usd] float NOT NULL, 
	[date_key] bigint NULL
);