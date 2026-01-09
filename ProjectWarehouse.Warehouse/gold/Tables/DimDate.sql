CREATE TABLE [gold].[DimDate] (

	[date_key] int NOT NULL, 
	[date] date NOT NULL, 
	[year] int NULL, 
	[quarter] int NULL, 
	[month] int NULL, 
	[month_name] varchar(20) NULL, 
	[day_of_month] int NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(20) NULL, 
	[week_of_year] int NULL, 
	[is_weekend] bit NULL
);