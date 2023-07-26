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

CREATE EXTERNAL TABLE dbo.dimRider
WITH (
    LOCATION     = 'dimRider',
    DATA_SOURCE = [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net],
    FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT 
	[rider_id],
	[first],
	[last],
	[address],
	[birthday],
	[account_start_date],
	[account_end_date],
	[is_member]
FROM [dbo].[rider];

GO

SELECT TOP 10 * FROM dimRider