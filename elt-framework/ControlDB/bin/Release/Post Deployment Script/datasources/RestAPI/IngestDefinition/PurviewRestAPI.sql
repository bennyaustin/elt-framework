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