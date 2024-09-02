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
	'restatements' AS [StreamName],
	'Custom Document Intelligence Model output for Real Estate Statements' AS [SourceSystemDescription],
	'Azure AI DocI REST API' AS [Backend],
	'JSON' AS [DataFormat],
	'doci' AS [SourceFileDropFileSystem],
	'inference/re-statements' AS [SourceFileDropFolder],
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
	'form10q' AS [StreamName],
	'Custom Document Intelligence Model output for Form 10-Q' AS [SourceSystemDescription],
	'Azure AI DocI REST API' AS [Backend],
	'JSON' AS [DataFormat],
	'doci' AS [SourceFileDropFileSystem],
	'inference/form10q' AS [SourceFileDropFolder],
	'*.json' AS [SourceFileDropFile],
	'raw-bronze' AS [DestinationRawFileSystem],
	'form10q/YYYY/MM' AS [DestinationRawFolder],
	'YYYY-MM-DD_HHMISS_form10q.json' AS [DestinationRawFile],
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