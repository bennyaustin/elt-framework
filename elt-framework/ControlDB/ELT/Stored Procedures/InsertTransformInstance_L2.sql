CREATE PROCEDURE [ELT].[InsertTransformInstance_L2]
	--PK/FK
	@L2TransformID int = null,
	@IngestID int,
	@L1TransformID int,

	--Databricks Notebook
	@NotebookName varchar(100) = null,
	@NotebookPath varchar(200) = null,

	--Custom
	@CustomParameters varchar(max) = null,

	--Input File
	@InputFileSystem varchar(50) = null,
    @InputFileFolder varchar(200) = null,
    @InputFile varchar(200) = null,
    @InputFileDelimiter char(1) = null,
	@InputFileHeaderFlag bit = null,
	@InputDWTable varchar(200) = null,

	--Delta
	@DeltaName varchar(50) = null,
	@DataFromTimestamp Datetime2 =null,
	@DataToTimestamp Datetime2 =null,
	@DataFromNumber int =null,
	@DataToNumber int =null,
	@LastDeltaDate datetime = null,
	@LastDeltaNumber int = null,


	--Curated File 
	@OutputL2CurateFileSystem varchar(50),
    @OutputL2CuratedFolder varchar(200),
    @OutputL2CuratedFile varchar(200),
    @OutputL2CuratedFileDelimiter char(1) = null,
    @OutputL2CuratedFileFormat varchar(10) = null,
    @OutputL2CuratedFileWriteMode varchar(20) = null,

	--SQL
	@OutputDWStagingTable varchar(200) = null,
	@LookupColumns varchar(4000) = null,	
    @OutputDWTable varchar(200) = null,
    @OutputDWTableWriteMode varchar(20) = null,
	

	--ADF Pipeline IDs
	@IngestADFPipelineRunID uniqueidentifier = null,
	@L1TransformADFPipelineRunID  uniqueidentifier = null,

	--Max Retries
	@MaxRetries int = null

AS
BEGIN

DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

	--Check if Transformation records already exists for the input file for same transformation e.g it's a reload
		IF NOT EXISTS 
			(
				SELECT 1 
				FROM 
					[ELT].[L2TransformInstance]
				WHERE 
					[IngestID] = @IngestID
					AND L2TransformID = @L2TransformID
					--Check does curated file record already exist
					AND OutputL2CurateFileSystem = @OutputL2CurateFileSystem
					AND OutputL2CuratedFolder = @OutputL2CuratedFolder
					AND OutputL2CuratedFile = @OutputL2CuratedFile

			)


	BEGIN
	--If this is a new transformation
		INSERT INTO [ELT].[L2TransformInstance]
			(
				 [L2TransformID]
				,[IngestID]
				,[L1TransformID]
				,[NotebookPath]
				,[NotebookName]
				,[CustomParameters]
				,[InputFileSystem]
				,[InputFileFolder]
				,[InputFile]
				,[InputFileDelimiter]
				,[InputFileHeaderFlag]
				,[InputDWTable]
				,[DeltaName]
				,[OutputL2CurateFileSystem]
				,[OutputL2CuratedFolder]
				,[OutputL2CuratedFile]
				,[OutputL2CuratedFileDelimiter]
				,[OutputL2CuratedFileFormat]
				,[OutputL2CuratedFileWriteMode]
				,[OutputDWStagingTable]
				,[LookupColumns]
				,[OutputDWTable]
				,[OutputDWTableWriteMode]
				,[RetryCount]
				,[ActiveFlag]
				,[IngestADFPipelineRunID]
				,[L1TransformADFPipelineRunID] --1555
				,[CreatedBy]
				,[CreatedTimestamp]

			   
			)
		VALUES
			(
				@L2TransformID,
				@IngestID,
				@L1TransformID,
				@NotebookPath,
				@NotebookName,
				@CustomParameters,
				@InputFileSystem,
				@InputFileFolder,
				@InputFile,
				@InputFileDelimiter,
				@InputFileHeaderFlag,
				@InputDWTable,
				@DeltaName,
				@OutputL2CurateFileSystem,
				@OutputL2CuratedFolder,
				@OutputL2CuratedFile,
				@OutputL2CuratedFileDelimiter,
				@OutputL2CuratedFileFormat,
				@OutputL2CuratedFileWriteMode,
				@OutputDWStagingTable,
				@LookupColumns,
				@OutputDWTable,
				@OutputDWTableWriteMode,
				0,
				1,
				@IngestADFPipelineRunID,
				@L1TransformADFPipelineRunID,	--1555
				SUSER_NAME(),
				@localdate
		)
		END
	ELSE
		--If this is an existing Transformation
		BEGIN
			--Just update one record in case if there are duplicates
			UPDATE TOP (1) [ELT].[L2TransformInstance]
		SET 
			[InputCount] = null
			,[L2TransformCount] = null
			,[L2TransformStartTimestamp] = null
			,[L2TransformEndTimestamp] = null
			,[L2TransformStatus] = null
			,[RetryCount] = 0
			,[ActiveFlag] = 1
			,[ReRunL2TransformFlag] = 1
			,[IngestADFPipelineRunID] = @IngestADFPipelineRunID
			,[L1TransformADFPipelineRunID] = null
			,[L2TransformADFPipelineRunID] = null
			,[ModifiedBy] = suser_sname()
			,[ModifiedTimestamp] = @localdate
		WHERE 
			[IngestID] = @IngestID
			AND L2TransformID = @L2TransformID
			--Check does curated file record already exist
			AND OutputL2CurateFileSystem = @OutputL2CurateFileSystem
			AND OutputL2CuratedFolder = @OutputL2CuratedFolder
			AND OutputL2CuratedFile = @OutputL2CuratedFile
			AND (ActiveFlag = 0 AND ISNULL(RetryCount,0)  >= @MaxRetries)
				
		END
END