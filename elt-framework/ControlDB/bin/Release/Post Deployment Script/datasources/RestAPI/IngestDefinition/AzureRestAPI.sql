/*
ELT Configuration for REST API Data Sources
*/

IF OBJECT_ID('tempdb..#AzureRestAPI_Ingest') IS NOT NULL DROP TABLE #AzureRestAPI_Ingest;

--Create Temp table with same structure as IngestDefinition
CREATE TABLE #AzureRestAPI_Ingest
(
	[SourceSystemName] [varchar](50) NOT NULL,
	[StreamName] [varchar](100) NULL,
	[SourceSystemDescription] [varchar](200) NULL,
	[Backend] [varchar](30) NULL,
	[EntityName] [varchar](100) NULL,
	[DeltaName] [varchar](50) NULL,
	[LastDeltaDate] [datetime2](7) NULL,
	[LastDeltaNumber] [int] NULL,
	[LastDeltaString] [varchar](50) NULL,
	[MaxIntervalMinutes] [int] NULL,
	[MaxIntervalNumber] [int] NULL,
	[DataMapping] [varchar](max) NULL,
	[DestinationRawFileSystem] [varchar](50) NULL,
	[DestinationRawFolder] [varchar](200) NULL,
	[DestinationRawFile] [varchar](200) NULL,
	[RunSequence] [int] NULL,
	[MaxRetries] [int] NULL,
	[ActiveFlag] [bit] NOT NULL,
	[L1TransformationReqdFlag] [bit] NOT NULL,
	[L2TransformationReqdFlag] [bit] NOT NULL,
	[DelayL1TransformationFlag] [bit] NOT NULL,
	[DelayL2TransformationFlag] [bit] NOT NULL
);


--Insert Into Temp Table
INSERT INTO #AzureRestAPI_Ingest
--Operations
SELECT 'Azure' AS	[SourceSystemName],
	'operations' AS [StreamName],
	'Azure APIs' AS [SourceSystemDescription],
	'AZURE REST API' AS [Backend],
	'/providers/Microsoft.Purview/operations?api-version=2020-12-01-preview' AS [EntityName],
	NULL AS [DeltaName],
	NULL AS [LastDeltaDate],
	NULL AS [LastDeltaNumber],
	NULL AS [LastDeltaString],
	NULL AS [MaxIntervalMinutes],
	NULL AS [MaxIntervalNumber],
	'{
    "type": "TabularTranslator",
    "mappings": [
        {
            "source": {
                "path": "[''name'']"
            },
            "sink": {
                "name": "name",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''display''][''provider'']"
            },
            "sink": {
                "name": "provider",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''display''][''resource'']"
            },
            "sink": {
                "name": "resource",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''display''][''operation'']"
            },
            "sink": {
                "name": "operation",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''display''][''description'']"
            },
            "sink": {
                "name": "description",
                "type": "String"
            }
        }
    ],
    "collectionReference": "$[''value'']",
    "mapComplexValuesToString": true
    }'AS [DataMapping],
	'raw-bronze' AS [DestinationRawFileSystem],
	'purview/operations/YYYY-MM'  AS [DestinationRawFolder] ,
	'operations_YYYYMMDD.parquet' AS [DestinationRawFile],
	1 AS [RunSequence],
	3 AS [MaxRetries],
	1 AS [ActiveFlag],
	1 AS [L1TransformationReqdFlag],
	1 AS [L2TransformationReqdFlag],
	1 AS [DelayL1TransformationFlag],
	1 AS [DelayL2TransformationFlag] ;

--Merge with Temp table for re-runnability

MERGE INTO [ELT].[IngestDefinition] AS tgt
USING #AzureRestAPI_Ingest AS src
ON src.[SourceSystemName]=tgt.[SourceSystemName] AND src.[StreamName] =tgt.[StreamName]
WHEN MATCHED THEN
    UPDATE SET tgt.[SourceSystemDescription] = src.[SourceSystemDescription],
               tgt.[Backend] = src.[Backend],
               tgt.[EntityName] = src.[EntityName],
               tgt.[DeltaName] = src.[DeltaName],
               tgt.[LastDeltaDate] = src.[LastDeltaDate],
               tgt.[LastDeltaNumber] = src.[LastDeltaNumber],
	           tgt.[LastDeltaString] =  src.[LastDeltaString],
               tgt.[MaxIntervalMinutes] = src.[MaxIntervalMinutes],
	           tgt.[MaxIntervalNumber] = src.[MaxIntervalNumber],
	           tgt.[DataMapping] = src.[DataMapping],
	           tgt.[DestinationRawFileSystem] = src.[DestinationRawFileSystem],
	           tgt.[DestinationRawFolder] = src.[DestinationRawFolder],
	           tgt.[DestinationRawFile] =  src.[DestinationRawFile],
	           tgt.[RunSequence] = src.[RunSequence],
	           tgt.[MaxRetries] = src.[MaxRetries],
	           tgt.[ActiveFlag] = src.[ActiveFlag],
	           tgt.[L1TransformationReqdFlag]  = src.[L1TransformationReqdFlag],
	           tgt.[L2TransformationReqdFlag] = src.[L2TransformationReqdFlag],
	           tgt.[DelayL1TransformationFlag] = src.[DelayL1TransformationFlag],
	           tgt.[DelayL2TransformationFlag] = src.[DelayL2TransformationFlag],
               tgt.[ModifiedBy] = USER_NAME(),
               tgt.[ModifiedTimestamp] = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT([SourceSystemName],
	        [StreamName],
	        [SourceSystemDescription],
	        [Backend],
	        [EntityName],
	        [DeltaName],
	        [LastDeltaDate],
	        [LastDeltaNumber],
	        [LastDeltaString],
	        [MaxIntervalMinutes],
	        [MaxIntervalNumber],
	        [DataMapping],
	        [DestinationRawFileSystem],
	        [DestinationRawFolder],
	        [DestinationRawFile],
	        [RunSequence],
	        [MaxRetries],
	        [ActiveFlag],
	        [L1TransformationReqdFlag],
	        [L2TransformationReqdFlag],
	        [DelayL1TransformationFlag],
	        [DelayL2TransformationFlag],
            [CreatedBy],
	        [CreatedTimestamp] )
    VALUES (src.[SourceSystemName],
	        src.[StreamName],
	        src.[SourceSystemDescription],
	        src.[Backend],
	        src.[EntityName],
	        src.[DeltaName],
	        src.[LastDeltaDate],
	        src.[LastDeltaNumber],
	        src.[LastDeltaString],
	        src.[MaxIntervalMinutes],
	        src.[MaxIntervalNumber],
	        src.[DataMapping],
	        src.[DestinationRawFileSystem],
	        src.[DestinationRawFolder],
	        src.[DestinationRawFile],
	        src.[RunSequence],
	        src.[MaxRetries],
	        src.[ActiveFlag],
	        src.[L1TransformationReqdFlag],
	        src.[L2TransformationReqdFlag],
	        src.[DelayL1TransformationFlag],
	        src.[DelayL2TransformationFlag],
            USER_NAME(),
	        GETDATE());