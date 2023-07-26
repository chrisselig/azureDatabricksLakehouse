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


/* Create joined reference table first for factTrip table */
CREATE EXTERNAL TABLE dbo.refTrip
WITH (
    LOCATION     = 'refTrip',
    DATA_SOURCE = [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net],
    FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT
	t.trip_id,
	REPLACE(LEFT(t.start_at,10),'-','') as date_id,
    r.rider_id,
	t.start_station_id,
    t.end_station_id,
    DATEDIFF(second, t.start_at, t.ended_at) AS [duration],
    FLOOR(DATEDIFF(year, r.birthday, GETDATE())) as rider_age
FROM trip t
JOIN rider r ON t.rider_id = r.rider_id

GO

/* Create factPayment table */
CREATE EXTERNAL TABLE dbo.factTrip
WITH (
    LOCATION     = 'factTrip',
    DATA_SOURCE = [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net],
    FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT
	trip_id,
	date_id,
	rider_id,
    start_station_id,
    end_station_id,
    duration,
    rider_age
FROM refTrip

GO

SELECT TOP 100 * FROM factTrip