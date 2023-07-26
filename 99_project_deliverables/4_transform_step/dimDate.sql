IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://dataengineeringfilesystem@dataengineeringstoragecs.dfs.core.windows.net' 
	)
GO

/* Create date dimension table */
CREATE EXTERNAL TABLE dbo.dimDate
WITH (
    LOCATION     = 'dimDate',
    DATA_SOURCE = [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net],
    FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
/* generates a list with 10 values */
WITH 
List AS (
    SELECT  
        n
    FROM
        (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
),

/* generates 10k rows by cross joining List with itself */
Range AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL))-1 AS RowNumber
    FROM
        List a
        CROSS JOIN List b
        CROSS JOIN List c
        CROSS JOIN List d
),

/* query to specify date range */
DateRange AS (
    SELECT
        CAST(CONCAT(YEAR(GETDATE())-4,'-01-01') AS DATE) AS StartDate /* should always start on 1st January */,
        CAST(CONCAT(YEAR(GETDATE())+5, '-12-31') AS DATE) AS EndDate /* should always end on 31st December*/
),

/* query to generate dates between given start date and end date */
Calendar AS (
SELECT
    DATEADD(DAY, r.RowNumber, dr.StartDate ) AS Date
FROM
    Range r
    CROSS JOIN DateRange dr
WHERE
    DATEADD(DAY, r.RowNumber, dr.StartDate ) <= dr.EndDate
)

/* date table query */
SELECT
	CAST(FORMAT(c.Date,'yyyymmdd') as int) as [date_id],
    c.Date as [date],
    YEAR(c.Date) AS [year],
    MONTH(c.Date) AS [month_number],
    DATENAME(MONTH, c.Date) AS [month_name],
    DAY(c.Date) AS [day_number],
    DATENAME(WEEKDAY, c.Date) AS [day_of_week],
	DATEPART(QUARTER, c.Date) AS [quarter]
FROM
    Calendar c
ORDER BY
    c.Date

GO

SELECT TOP 10 * FROM dimDate