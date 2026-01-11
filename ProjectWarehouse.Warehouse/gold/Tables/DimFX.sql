CREATE TABLE [gold].[DimFX] (

	[fx_key] bigint IDENTITY NOT NULL, 
	[currency] varchar(10) NOT NULL, 
	[currency_denom] varchar(10) NOT NULL, 
	[time_period] date NOT NULL, 
	[obs_value] float NOT NULL, 
	[date_key] bigint NULL
);