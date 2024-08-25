/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/

--IngestDefinition
/*
ELT Configuration for REST API Data Sources
*/

IF OBJECT_ID('tempdb..#PurviewRestAPI_Ingest') IS NOT NULL DROP TABLE #PurviewRestAPI_Ingest;

--Create Temp table with same structure as IngestDefinition
CREATE TABLE #PurviewRestAPI_Ingest
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
INSERT INTO #PurviewRestAPI_Ingest
--datasources
SELECT 'Purview' AS	[SourceSystemName],
	'datasources' AS [StreamName],
	'Microsoft Purview APIs' AS [SourceSystemDescription],
	'ATLAS REST API' AS [Backend],
	'/scan/datasources?api-version=2022-02-01-preview' AS [EntityName],
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
                "path": "[''properties''][''endpoint'']"
            },
            "sink": {
                "name": "endpoint",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''resourceGroup'']"
            },
            "sink": {
                "name": "resourceGroup",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''subscriptionId'']"
            },
            "sink": {
                "name": "subscriptionId",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''location'']"
            },
            "sink": {
                "name": "location",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''resourceName'']"
            },
            "sink": {
                "name": "resourceName",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''resourceId'']"
            },
            "sink": {
                "name": "resourceId",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''dataUseGovernance'']"
            },
            "sink": {
                "name": "dataUseGovernance",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''createdAt'']"
            },
            "sink": {
                "name": "createdAt",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''lastModifiedAt'']"
            },
            "sink": {
                "name": "lastModifiedAt",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''parentCollection'']"
            },
            "sink": {
                "name": "parentCollection",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''collection''][''lastModifiedAt'']"
            },
            "sink": {
                "name": "Collection_lastModifiedAt",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''collection''][''referenceName'']"
            },
            "sink": {
                "name": "referenceName",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''collection''][''type'']"
            },
            "sink": {
                "name": "type",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''properties''][''dataSourceCollectionMovingState'']"
            },
            "sink": {
                "name": "dataSourceCollectionMovingState",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''kind'']"
            },
            "sink": {
                "name": "kind",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''id'']"
            },
            "sink": {
                "name": "id",
                "type": "String"
            }
        },
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
                "path": "$[''count'']"
            },
            "sink": {
                "name": "count",
                "type": "String"
            }
        }
    ],
    "collectionReference": "$[''value'']",
    "mapComplexValuesToString": true
    }'AS [DataMapping],
	'raw-bronze' AS [DestinationRawFileSystem],
	'purview/datasources/YYYY-MM'  AS [DestinationRawFolder] ,
	'datasources_YYYYMMDD.parquet' AS [DestinationRawFile],
	1 AS [RunSequence],
	3 AS [MaxRetries],
	1 AS [ActiveFlag],
	1 AS [L1TransformationReqdFlag],
	1 AS [L2TransformationReqdFlag],
	1 AS [DelayL1TransformationFlag],
	1 AS [DelayL2TransformationFlag]

UNION
--Collections
SELECT 'Purview' AS	[SourceSystemName],
	'collections' AS [StreamName],
	'Microsoft Purview APIs' AS [SourceSystemDescription],
	'ATLAS REST API' AS [Backend],
	'/collections?api-version=2019-11-01-preview' AS [EntityName],
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
                "path": "[''friendlyName'']"
            },
            "sink": {
                "name": "friendlyName",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''description'']"
            },
            "sink": {
                "name": "description",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''systemData''][''createdBy'']"
            },
            "sink": {
                "name": "createdBy",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''systemData''][''createdByType'']"
            },
            "sink": {
                "name": "createdByType",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''systemData''][''createdAt'']"
            },
            "sink": {
                "name": "createdAt",
                "type": "DateTime"
            }
        },
        {
            "source": {
                "path": "[''systemData''][''lastModifiedByType'']"
            },
            "sink": {
                "name": "lastModifiedByType",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''systemData''][''lastModifiedAt'']"
            },
            "sink": {
                "name": "lastModifiedAt",
                "type": "DateTime"
            }
        },
        {
            "source": {
                "path": "[''collectionProvisioningState'']"
            },
            "sink": {
                "name": "collectionProvisioningState",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''parentCollection''][''type'']"
            },
            "sink": {
                "name": "parentCollectionType",
                "type": "String"
            }
        },
        {
            "source": {
                "path": "[''parentCollection''][''referenceName'']"
            },
            "sink": {
                "name": "parentCollectionReferenceName",
                "type": "String"
            }
        }
    ],
    "collectionReference": "$[''value'']",
    "mapComplexValuesToString": true
    }' AS [DataMapping],
	'raw-bronze' AS [DestinationRawFileSystem],
	'purview/collections/YYYY-MM'  AS [DestinationRawFolder] ,
	'collections_YYYYMMDD.parquet' AS [DestinationRawFile],
	2 AS [RunSequence],
	3 AS [MaxRetries],
	1 AS [ActiveFlag],
	1 AS [L1TransformationReqdFlag],
	1 AS [L2TransformationReqdFlag],
	1 AS [DelayL1TransformationFlag],
	1 AS [DelayL2TransformationFlag]
    ;


--Merge with Temp table for re-runnability

MERGE INTO [ELT].[IngestDefinition] AS tgt
USING #PurviewRestAPI_Ingest AS src
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

GO
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
/*
ELT Configuration for CustomFormRecognizerModel JSON Data Source
*/

IF OBJECT_ID('tempdb..#CFRM_Ingest') IS NOT NULL DROP TABLE #CFRM_Ingest;

--Create Temp table with similar schema as IngestDefinition
CREATE TABLE #CFRM_Ingest
(
	[SourceSystemName] [varchar](50) NOT NULL,
	[StreamName] [varchar](100) NULL,
	[SourceSystemDescription] [varchar](200) NULL,
	[Backend] [varchar](30) NULL,
	[DataFormat] [varchar](10) NULL,
	[SourceFileDropFileSystem] [varchar](50) NULL,
	[SourceFileDropFolder] [varchar](200) NULL,
	[SourceFileDropFile] [varchar](200) NULL,
	[DestinationRawFileSystem] [varchar](50) NULL,
	[DestinationRawFolder] [varchar](200) NULL,
	[DestinationRawFile] [varchar](200) NULL,
	[ActiveFlag] [bit] NOT NULL,
	[L1TransformationReqdFlag] [bit] NOT NULL,
	[L2TransformationReqdFlag] [bit] NOT NULL,
	[DelayL1TransformationFlag] [bit] NOT NULL,
	[DelayL2TransformationFlag] [bit] NOT NULL
);


--Insert Into Temp Table
INSERT INTO #CFRM_Ingest
--Operations
SELECT 'AIML-OCR' AS	[SourceSystemName],
	'analyze' AS [StreamName],
	'Custom Form Recognizer Model output for Real Estate Agency' AS [SourceSystemDescription],
	'Form Recognizer REST API' AS [Backend],
	'JSON' AS [DataFormat],
	'inference' AS [SourceFileDropFileSystem],
	'formrecognizer-re-json' AS [SourceFileDropFolder],
	'*.json' AS [SourceFileDropFile],
	'raw-bronze' AS [DestinationRawFileSystem],
	're-statements/YYYY/MM' AS [DestinationRawFolder],
	'YYYY-MM-DD_HHMISS_re-statement.json' AS [DestinationRawFile],
	1 AS [ActiveFlag],
	1 AS [L1TransformationReqdFlag],
	0 AS [L2TransformationReqdFlag],
	0 AS [DelayL1TransformationFlag],
	1 AS [DelayL2TransformationFlag]
UNION
SELECT 'AIML-OCR' AS	[SourceSystemName],
	'analyze-sec-form10q' AS [StreamName],
	'Custom Form Recognizer Model output for SEC Form 10-Q' AS [SourceSystemDescription],
	'Form Recognizer REST API' AS [Backend],
	'JSON' AS [DataFormat],
	'inference' AS [SourceFileDropFileSystem],
	'formrecognizer-sec-form10q-json' AS [SourceFileDropFolder],
	'*.json' AS [SourceFileDropFile],
	'raw-bronze' AS [DestinationRawFileSystem],
	'sec-form10q/YYYY/MM' AS [DestinationRawFolder],
	'YYYY-MM-DD_HHMISS_sec-form10q.json' AS [DestinationRawFile],
	1 AS [ActiveFlag],
	1 AS [L1TransformationReqdFlag],
	0 AS [L2TransformationReqdFlag],
	0 AS [DelayL1TransformationFlag],
	1 AS [DelayL2TransformationFlag]

--Merge with Temp table for re-runnability

MERGE INTO [ELT].[IngestDefinition] AS tgt
USING #CFRM_Ingest AS src
ON src.[SourceSystemName]=tgt.[SourceSystemName] AND src.[StreamName]=tgt.[StreamName]
WHEN MATCHED THEN
    UPDATE SET tgt.[SourceSystemDescription] = src.[SourceSystemDescription],
               tgt.[Backend] = src.[Backend],
			   tgt.[DataFormat] = src.[DataFormat],
			   tgt.[SourceFileDropFileSystem] = src.[SourceFileDropFileSystem],
			   tgt.[SourceFileDropFolder] =src.[SourceFileDropFolder],
			   tgt.[SourceFileDropFile] = src.[SourceFileDropFile],
	           tgt.[DestinationRawFileSystem] = src.[DestinationRawFileSystem],
	           tgt.[DestinationRawFolder] = src.[DestinationRawFolder],
	           tgt.[DestinationRawFile] =  src.[DestinationRawFile],
	           tgt.[ActiveFlag] = src.[ActiveFlag],
	           tgt.[L1TransformationReqdFlag]  = src.[L1TransformationReqdFlag],
	           tgt.[L2TransformationReqdFlag] = src.[L2TransformationReqdFlag],
	           tgt.[DelayL1TransformationFlag] = src.[DelayL1TransformationFlag],
	           tgt.[DelayL2TransformationFlag] = src.[DelayL2TransformationFlag],
               tgt.[ModifiedBy] = USER_NAME(),
               tgt.[ModifiedTimestamp] = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT(	[SourceSystemName],
			[StreamName],
			[SourceSystemDescription],
			[Backend],
			[DataFormat],
			[SourceFileDropFileSystem],
			[SourceFileDropFolder],
			[SourceFileDropFile],
			[DestinationRawFileSystem],
			[DestinationRawFolder],
			[DestinationRawFile],
			[ActiveFlag],
			[L1TransformationReqdFlag],
			[L2TransformationReqdFlag],
			[DelayL1TransformationFlag],
			[DelayL2TransformationFlag],
			[ModifiedBy],
			[ModifiedTimestamp])
    VALUES (src.[SourceSystemName],
			src.[StreamName],
			src.[SourceSystemDescription],
			src.[Backend],
			src.[DataFormat],
			src.[SourceFileDropFileSystem],
			src.[SourceFileDropFolder],
			src.[SourceFileDropFile],
			src.[DestinationRawFileSystem],
			src.[DestinationRawFolder],
			src.[DestinationRawFile],
			src.[ActiveFlag],
			src.[L1TransformationReqdFlag],
			src.[L2TransformationReqdFlag],
			src.[DelayL1TransformationFlag],
			src.[DelayL2TransformationFlag],
            USER_NAME(),
	        GETDATE());

--L1TransformDefinition
/*
ELT Configuration for REST API Data Sources
*/
Declare @SourceSystem VARCHAR(50), @StreamName VARCHAR(100)
SET @SourceSystem='Purview'
SET @StreamName ='%'

IF OBJECT_ID('tempdb..#PurviewRestAPI_L1') IS NOT NULL DROP TABLE #PurviewRestAPI_L1;
--Create Temp table with same structure as L1TransformDefinition
CREATE TABLE #PurviewRestAPI_L1
(
	[IngestID] int not null,
	[NotebookPath] varchar(200) null,
	[NotebookName] varchar(100) null,
	[CustomParameters] varchar(max) null,
	[InputRawFileSystem] varchar(50) not null,
	[InputRawFileFolder] varchar(200) not null,
	[InputRawFile] varchar(200) not null,
	[InputRawFileDelimiter] char(1) null,
	[InputFileHeaderFlag] bit null,
	[OutputL1CurateFileSystem] varchar(50) not null,
	[OutputL1CuratedFolder] varchar(200) not null,
	[OutputL1CuratedFile] varchar(200) not null,
	[OutputL1CuratedFileDelimiter] char(1) null,
	[OutputL1CuratedFileFormat] varchar(10) null,
	[OutputL1CuratedFileWriteMode] varchar(20) null,
	[OutputDWStagingTable] varchar(200) null,
	[LookupColumns] varchar(4000) null,
	[OutputDWTable] varchar(200) null,
	[OutputDWTableWriteMode] varchar(20) null,
	[MaxRetries] int null,
	[DeltaName] varchar(50) null,
	[ActiveFlag] bit not null
);

--Insert Into Temp Table
INSERT INTO #PurviewRestAPI_L1
	SELECT  [IngestID]
	,'L1Transform' AS [NotebookPath]
	,'L1Transform-Generic-Synapse' AS [NotebookName]
	, NULL AS [CustomParameters]
	,[DestinationRawFileSystem] AS [InputRawFileSystem]
	,[DestinationRawFolder] AS [InputRawFileFolder]
	,[DestinationRawFile] AS [InputRawFile]
	, NULL AS [InputRawFileDelimiter]
	, 1 AS [InputFileHeaderFlag] 
	, 'curated-silver' AS [OutputL1CurateFileSystem]
	, [DestinationRawFolder] AS [OutputL1CuratedFolder]
	, 'standardized_'+ [DestinationRawFile] AS [OutputL1CuratedFile]
	, NULL AS [OutputL1CuratedFileDelimiter] 
	, 'parquet' AS [OutputL1CuratedFileFormat]
	, 'overwrite' AS [OutputL1CuratedFileWriteMode]
	, 'stg.merge_' + [SourceSystemName] +'_'+ StreamName AS [OutputDWStagingTable] 
	, CASE WHEN StreamName = 'datasources' THEN '[''resourceName'']'
			WHEN StreamName = 'collections' THEN '[''name'']'
		END AS [LookupColumns]
	, SourceSystemName + '.' + StreamName AS [OutputDWTable]
	, 'append' AS [OutputDWTableWriteMode]
	,3 AS [MaxRetries]
	,  CASE WHEN StreamName = 'datasources' THEN 'lastModifiedAt'
			WHEN StreamName = 'collections' THEN 'lastModifiedAt'
		END AS [DeltaName]
	, 1 AS [ActiveFlag]
	FROM  [ELT].[IngestDefinition]
	WHERE [SourceSystemName]=@SourceSystem
	AND [StreamName] like @StreamName;


--Merge with Temp table for re-runnability

MERGE INTO [ELT].[L1TransformDefinition] AS tgt
USING #PurviewRestAPI_L1 AS src
ON src.[InputRawFileSystem] = tgt.[InputRawFileSystem]
 AND src.[InputRawFileFolder] = tgt.[InputRawFileFolder]
 AND src.[InputRawFile] = tgt.[InputRawFile]
 AND src.[OutputL1CurateFileSystem] = tgt.[OutputL1CurateFileSystem]
 AND src.[OutputL1CuratedFolder] = tgt.[OutputL1CuratedFolder]
 AND src.[OutputL1CuratedFile] = tgt.[OutputL1CuratedFile]
WHEN MATCHED THEN
    UPDATE SET tgt.[IngestID] =src.[IngestID],
			tgt.[NotebookPath] =src.[NotebookPath],
			tgt.[NotebookName] =src.[NotebookName],
			tgt.[CustomParameters] =src.[CustomParameters],
			tgt.[InputRawFileSystem] =src.[InputRawFileSystem],
			tgt.[InputRawFileFolder] =src.[InputRawFileFolder],
			tgt.[InputRawFile] =src.[InputRawFile],
			tgt.[InputRawFileDelimiter] =src.[InputRawFileDelimiter],
			tgt.[InputFileHeaderFlag] =src.[InputFileHeaderFlag],
			tgt.[OutputL1CurateFileSystem] =src.[OutputL1CurateFileSystem],
			tgt.[OutputL1CuratedFolder] =src.[OutputL1CuratedFolder],
			tgt.[OutputL1CuratedFile] =src.[OutputL1CuratedFile],
			tgt.[OutputL1CuratedFileDelimiter] =src.[OutputL1CuratedFileDelimiter],
			tgt.[OutputL1CuratedFileFormat] =src.[OutputL1CuratedFileFormat],
			tgt.[OutputL1CuratedFileWriteMode] =src.[OutputL1CuratedFileWriteMode],
			tgt.[OutputDWStagingTable] =src.[OutputDWStagingTable],
			tgt.[LookupColumns] =src.[LookupColumns],
			tgt.[OutputDWTable] =src.[OutputDWTable],
			tgt.[OutputDWTableWriteMode] =src.[OutputDWTableWriteMode],
			tgt.[MaxRetries] =src.[MaxRetries],
			tgt.[DeltaName] =src.[DeltaName],
			tgt.[ActiveFlag] =src.[ActiveFlag],
            tgt.[ModifiedBy] = USER_NAME(),
            tgt.[ModifiedTimestamp] = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT([IngestID],
			[NotebookPath],
			[NotebookName],
			[CustomParameters],
			[InputRawFileSystem],
			[InputRawFileFolder],
			[InputRawFile],
			[InputRawFileDelimiter],
			[InputFileHeaderFlag],
			[OutputL1CurateFileSystem],
			[OutputL1CuratedFolder],
			[OutputL1CuratedFile],
			[OutputL1CuratedFileDelimiter],
			[OutputL1CuratedFileFormat],
			[OutputL1CuratedFileWriteMode],
			[OutputDWStagingTable],
			[LookupColumns],
			[OutputDWTable],
			[OutputDWTableWriteMode],
			[MaxRetries],
			[DeltaName],
			[ActiveFlag],
            [CreatedBy],
	        [CreatedTimestamp] )
    VALUES (src.[IngestID],
			src.[NotebookPath],
			src.[NotebookName],
			src.[CustomParameters],
			src.[InputRawFileSystem],
			src.[InputRawFileFolder],
			src.[InputRawFile],
			src.[InputRawFileDelimiter],
			src.[InputFileHeaderFlag],
			src.[OutputL1CurateFileSystem],
			src.[OutputL1CuratedFolder],
			src.[OutputL1CuratedFile],
			src.[OutputL1CuratedFileDelimiter],
			src.[OutputL1CuratedFileFormat],
			src.[OutputL1CuratedFileWriteMode],
			src.[OutputDWStagingTable],
			src.[LookupColumns],
			src.[OutputDWTable],
			src.[OutputDWTableWriteMode],
			src.[MaxRetries],
			src.[DeltaName],
			src.[ActiveFlag],
            USER_NAME(),
	        GETDATE());

GO
/*
ELT Configuration for REST API Data Sources
*/
Declare @SourceSystem VARCHAR(50), @StreamName VARCHAR(100)
SET @SourceSystem='Azure'
SET @StreamName ='%'

IF OBJECT_ID('tempdb..#AzureRestAPI_L1') IS NOT NULL DROP TABLE #AzureRestAPI_L1;
--Create Temp table with same structure as L1TransformDefinition
CREATE TABLE #AzureRestAPI_L1
(
	[IngestID] int not null,
	[NotebookPath] varchar(200) null,
	[NotebookName] varchar(100) null,
	[CustomParameters] varchar(max) null,
	[InputRawFileSystem] varchar(50) not null,
	[InputRawFileFolder] varchar(200) not null,
	[InputRawFile] varchar(200) not null,
	[InputRawFileDelimiter] char(1) null,
	[InputFileHeaderFlag] bit null,
	[OutputL1CurateFileSystem] varchar(50) not null,
	[OutputL1CuratedFolder] varchar(200) not null,
	[OutputL1CuratedFile] varchar(200) not null,
	[OutputL1CuratedFileDelimiter] char(1) null,
	[OutputL1CuratedFileFormat] varchar(10) null,
	[OutputL1CuratedFileWriteMode] varchar(20) null,
	[OutputDWStagingTable] varchar(200) null,
	[LookupColumns] varchar(4000) null,
	[OutputDWTable] varchar(200) null,
	[OutputDWTableWriteMode] varchar(20) null,
	[MaxRetries] int null,
	[DeltaName] varchar(50) null,
	[ActiveFlag] bit not null
);

--Insert Into Temp Table
INSERT INTO #AzureRestAPI_L1
	SELECT  [IngestID]
	,'L1Transform' AS [NotebookPath]
	,'L1Transform-Generic-Synapse' AS [NotebookName]
	, NULL AS [CustomParameters]
	,[DestinationRawFileSystem] AS [InputRawFileSystem]
	,[DestinationRawFolder] AS [InputRawFileFolder]
	,[DestinationRawFile] AS [InputRawFile]
	, NULL AS [InputRawFileDelimiter]
	, 1 AS [InputFileHeaderFlag] 
	, 'curated-silver' AS [OutputL1CurateFileSystem]
	, [DestinationRawFolder] AS [OutputL1CuratedFolder]
	, 'standardized_'+ [DestinationRawFile] AS [OutputL1CuratedFile]
	, NULL AS [OutputL1CuratedFileDelimiter] 
	, 'parquet' AS [OutputL1CuratedFileFormat]
	, 'overwrite' AS [OutputL1CuratedFileWriteMode]
	, 'stg.merge_' + [SourceSystemName] +'_'+ StreamName AS [OutputDWStagingTable] 
	, CASE WHEN StreamName = 'operations' THEN '[''name'']'
			ELSE NULL
		END AS [LookupColumns]
	, SourceSystemName + '.' + StreamName AS [OutputDWTable]
	, 'append' AS [OutputDWTableWriteMode]
	,3 AS [MaxRetries]
	,  NULL AS [DeltaName]
	, 1 AS [ActiveFlag]
	FROM  [ELT].[IngestDefinition]
	WHERE [SourceSystemName]=@SourceSystem
	AND [StreamName] like @StreamName;


--Merge with Temp table for re-runnability

MERGE INTO [ELT].[L1TransformDefinition] AS tgt
USING #AzureRestAPI_L1 AS src
ON src.[InputRawFileSystem] = tgt.[InputRawFileSystem]
 AND src.[InputRawFileFolder] = tgt.[InputRawFileFolder]
 AND src.[InputRawFile] = tgt.[InputRawFile]
 AND src.[OutputL1CurateFileSystem] = tgt.[OutputL1CurateFileSystem]
 AND src.[OutputL1CuratedFolder] = tgt.[OutputL1CuratedFolder]
 AND src.[OutputL1CuratedFile] = tgt.[OutputL1CuratedFile]
WHEN MATCHED THEN
    UPDATE SET tgt.[IngestID] =src.[IngestID],
			tgt.[NotebookPath] =src.[NotebookPath],
			tgt.[NotebookName] =src.[NotebookName],
			tgt.[CustomParameters] =src.[CustomParameters],
			tgt.[InputRawFileSystem] =src.[InputRawFileSystem],
			tgt.[InputRawFileFolder] =src.[InputRawFileFolder],
			tgt.[InputRawFile] =src.[InputRawFile],
			tgt.[InputRawFileDelimiter] =src.[InputRawFileDelimiter],
			tgt.[InputFileHeaderFlag] =src.[InputFileHeaderFlag],
			tgt.[OutputL1CurateFileSystem] =src.[OutputL1CurateFileSystem],
			tgt.[OutputL1CuratedFolder] =src.[OutputL1CuratedFolder],
			tgt.[OutputL1CuratedFile] =src.[OutputL1CuratedFile],
			tgt.[OutputL1CuratedFileDelimiter] =src.[OutputL1CuratedFileDelimiter],
			tgt.[OutputL1CuratedFileFormat] =src.[OutputL1CuratedFileFormat],
			tgt.[OutputL1CuratedFileWriteMode] =src.[OutputL1CuratedFileWriteMode],
			tgt.[OutputDWStagingTable] =src.[OutputDWStagingTable],
			tgt.[LookupColumns] =src.[LookupColumns],
			tgt.[OutputDWTable] =src.[OutputDWTable],
			tgt.[OutputDWTableWriteMode] =src.[OutputDWTableWriteMode],
			tgt.[MaxRetries] =src.[MaxRetries],
			tgt.[DeltaName] =src.[DeltaName],
			tgt.[ActiveFlag] =src.[ActiveFlag],
            tgt.[ModifiedBy] = USER_NAME(),
            tgt.[ModifiedTimestamp] = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT([IngestID],
			[NotebookPath],
			[NotebookName],
			[CustomParameters],
			[InputRawFileSystem],
			[InputRawFileFolder],
			[InputRawFile],
			[InputRawFileDelimiter],
			[InputFileHeaderFlag],
			[OutputL1CurateFileSystem],
			[OutputL1CuratedFolder],
			[OutputL1CuratedFile],
			[OutputL1CuratedFileDelimiter],
			[OutputL1CuratedFileFormat],
			[OutputL1CuratedFileWriteMode],
			[OutputDWStagingTable],
			[LookupColumns],
			[OutputDWTable],
			[OutputDWTableWriteMode],
			[MaxRetries],
			[DeltaName],
			[ActiveFlag],
            [CreatedBy],
	        [CreatedTimestamp] )
    VALUES (src.[IngestID],
			src.[NotebookPath],
			src.[NotebookName],
			src.[CustomParameters],
			src.[InputRawFileSystem],
			src.[InputRawFileFolder],
			src.[InputRawFile],
			src.[InputRawFileDelimiter],
			src.[InputFileHeaderFlag],
			src.[OutputL1CurateFileSystem],
			src.[OutputL1CuratedFolder],
			src.[OutputL1CuratedFile],
			src.[OutputL1CuratedFileDelimiter],
			src.[OutputL1CuratedFileFormat],
			src.[OutputL1CuratedFileWriteMode],
			src.[OutputDWStagingTable],
			src.[LookupColumns],
			src.[OutputDWTable],
			src.[OutputDWTableWriteMode],
			src.[MaxRetries],
			src.[DeltaName],
			src.[ActiveFlag],
            USER_NAME(),
	        GETDATE());

GO
/*
ELT Configuration for CustomFormRecognizerModel JSON Data Source
*/
Declare @SourceSystem VARCHAR(50), @StreamName VARCHAR(100)
SET @SourceSystem='AIML-OCR'
SET @StreamName ='%'

IF OBJECT_ID('tempdb..#CFRM_L1') IS NOT NULL DROP TABLE #CFRM_L1;
--Create Temp table with same structure as L1TransformDefinition
CREATE TABLE #CFRM_L1
(
	[IngestID] int not null,
	[NotebookPath] varchar(200) null,
	[NotebookName] varchar(100) null,
	[InputRawFileSystem] varchar(50) not null,
	[InputRawFileFolder] varchar(200) not null,
	[InputRawFile] varchar(200) not null,
	[OutputL1CurateFileSystem] varchar(50) not null,
	[OutputL1CuratedFolder] varchar(200) not null,
	[OutputL1CuratedFile] varchar(200) not null,
	[OutputL1CuratedFileFormat] varchar(10) null,
	[OutputL1CuratedFileWriteMode] varchar(20) null,
	[OutputDWStagingTable] varchar(200) null,
	[LookupColumns] varchar(4000) null,
	[OutputDWTable] varchar(200) null,
	[OutputDWTableWriteMode] varchar(20) null,
	[ActiveFlag] bit not null
);

--Insert Into Temp Table
INSERT INTO #CFRM_L1
	SELECT  [IngestID]
	,'L1Transform' AS [NotebookPath]
	,'L1Transform-ReStatement' AS [NotebookName]
	,[DestinationRawFileSystem] AS [InputRawFileSystem]
	,[DestinationRawFolder] AS [InputRawFileFolder]
	,[DestinationRawFile] AS [InputRawFile]
	, 'curated-silver' AS [OutputL1CurateFileSystem]
	, [DestinationRawFolder] AS [OutputL1CuratedFolder]
	, 'standardized_'+ [DestinationRawFile] AS [OutputL1CuratedFile]
	, 'json' AS [OutputL1CuratedFileFormat]
	, 'overwrite' AS [OutputL1CuratedFileWriteMode]
	, null as [OutputDWStagingTable]
	, null as [LookupColumns]
	, null as [OutputDWTable]
	, null as [OutputDWTableWriteMode]
	, 1 AS [ActiveFlag]
	FROM  [ELT].[IngestDefinition]
	WHERE [SourceSystemName]=@SourceSystem
	AND [StreamName] ='analyze'

	UNION
	SELECT  [IngestID]
	,'L1Transform' AS [NotebookPath]
	,'L1Transform-SEC-Form10Q' AS [NotebookName]
	,[DestinationRawFileSystem] AS [InputRawFileSystem]
	,[DestinationRawFolder] AS [InputRawFileFolder]
	,[DestinationRawFile] AS [InputRawFile]
	, 'curated-silver' AS [OutputL1CurateFileSystem]
	, [DestinationRawFolder] AS [OutputL1CuratedFolder]
	, 'standardized_'+ [DestinationRawFile] AS [OutputL1CuratedFile]
	, 'json' AS [OutputL1CuratedFileFormat]
	, 'overwrite' AS [OutputL1CuratedFileWriteMode]
	, '[stg].[merge_sec_form10q]' as [OutputDWStagingTable]
	, '[''org_name'',''reporting_quarter'']' as [LookupColumns]
	, '[sec].[form10q]' as [OutputDWTable]
	, 'append' as [OutputDWTableWriteMode]
	, 1 AS [ActiveFlag]
	FROM  [ELT].[IngestDefinition]
	WHERE [SourceSystemName]=@SourceSystem
	AND [StreamName] ='analyze-sec-form10q'
--Merge with Temp table for re-runnability

MERGE INTO [ELT].[L1TransformDefinition] AS tgt
USING #CFRM_L1 AS src
ON src.[InputRawFileSystem] = tgt.[InputRawFileSystem]
 AND src.[InputRawFileFolder] = tgt.[InputRawFileFolder]
 AND src.[InputRawFile] = tgt.[InputRawFile]
 AND src.[OutputL1CurateFileSystem] = tgt.[OutputL1CurateFileSystem]
 AND src.[OutputL1CuratedFolder] = tgt.[OutputL1CuratedFolder]
 AND src.[OutputL1CuratedFile] = tgt.[OutputL1CuratedFile]
WHEN MATCHED THEN
    UPDATE SET tgt.[IngestID] =src.[IngestID],
			tgt.[NotebookPath] =src.[NotebookPath],
			tgt.[NotebookName] =src.[NotebookName],
			tgt.[InputRawFileSystem] =src.[InputRawFileSystem],
			tgt.[InputRawFileFolder] =src.[InputRawFileFolder],
			tgt.[InputRawFile] =src.[InputRawFile],
			tgt.[OutputL1CurateFileSystem] =src.[OutputL1CurateFileSystem],
			tgt.[OutputL1CuratedFolder] =src.[OutputL1CuratedFolder],
			tgt.[OutputL1CuratedFile] =src.[OutputL1CuratedFile],
			tgt.[OutputL1CuratedFileFormat] =src.[OutputL1CuratedFileFormat],
			tgt.[OutputL1CuratedFileWriteMode] =src.[OutputL1CuratedFileWriteMode],
			tgt.[ActiveFlag] =src.[ActiveFlag],
            tgt.[ModifiedBy] = USER_NAME(),
            tgt.[ModifiedTimestamp] = GetDate()
WHEN NOT MATCHED BY TARGET THEN
    INSERT([IngestID],
			[NotebookPath],
			[NotebookName],
			[InputRawFileSystem],
			[InputRawFileFolder],
			[InputRawFile],
			[OutputL1CurateFileSystem],
			[OutputL1CuratedFolder],
			[OutputL1CuratedFile],
			[OutputL1CuratedFileFormat],
			[OutputL1CuratedFileWriteMode],
			[ActiveFlag],
            [CreatedBy],
	        [CreatedTimestamp] )
    VALUES (src.[IngestID],
			src.[NotebookPath],
			src.[NotebookName],
			src.[InputRawFileSystem],
			src.[InputRawFileFolder],
			src.[InputRawFile],
			src.[OutputL1CurateFileSystem],
			src.[OutputL1CuratedFolder],
			src.[OutputL1CuratedFile],
			src.[OutputL1CuratedFileFormat],
			src.[OutputL1CuratedFileWriteMode],
			src.[ActiveFlag],
            USER_NAME(),
	        GETDATE());

GO
