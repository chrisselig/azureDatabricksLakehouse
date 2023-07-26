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

/* Create joined reference table first for factPayment table */
CREATE EXTERNAL TABLE dbo.refPayment
WITH (
    LOCATION     = 'refPayment',
    DATA_SOURCE = [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net],
    FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT
	p.payment_id,
	r.rider_id,
	d.date_id,
    p.amount
FROM payment p
JOIN rider r ON p.rider_id = r.rider_id
JOIN dimDate d ON p.date = d.date;

GO

/* Create factPayment table */
CREATE EXTERNAL TABLE dbo.factPayment
WITH (
    LOCATION     = 'factPayment',
    DATA_SOURCE = [dataengineeringfilesystem_dataengineeringstoragecs_dfs_core_windows_net],
    FILE_FORMAT = [SynapseDelimitedTextFormat]
)  
AS
SELECT
	payment_id,
	rider_id,
	date_id,
    amount
FROM refPayment

GO

SELECT TOP 100 * FROM factPayment