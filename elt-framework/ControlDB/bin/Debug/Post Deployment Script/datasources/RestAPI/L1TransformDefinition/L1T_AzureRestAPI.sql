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