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
	AND [StreamName] ='restatements'

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
	AND [StreamName] ='form10q'
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
			tgt.[OutputDWStagingTable] = src.[OutputDWStagingTable],
			tgt.[LookupColumns] = src.[LookupColumns],
			tgt.[OutputDWTable] = src.[OutputDWTable],
			tgt.[OutputDWTableWriteMode]=src.[OutputDWTableWriteMode],
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