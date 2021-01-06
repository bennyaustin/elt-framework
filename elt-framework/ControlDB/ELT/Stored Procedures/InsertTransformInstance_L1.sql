CREATE PROCEDURE [ELT].[InsertTransformInstance_L1]
	--PK/FK
	@L1TransformID int = null,
	@IngestInstanceID int=null,
	@IngestID int,

	--Databricks Notebook
	@NotebookName varchar(100) = null,
	@NotebookPath varchar(200) = null,
	
	--Custom
	@CustomParameters varchar(max) = null,

	--Input File
	@InputRawFileSystem varchar(50) = null,
    @InputRawFileFolder varchar(200) = null,
    @InputRawFile varchar(200) = null,
    @InputRawFileDelimiter char(1) = null,
	@InputFileHeaderFlag bit = null,
	
	--Curated File 
	@OutputL1CurateFileSystem varchar(50) = null,
    @OutputL1CuratedFolder varchar(200) = null,
    @OutputL1CuratedFile varchar(200) = null,
    @OutputL1CuratedFileDelimiter char(1) = null,
    @OutputL1CuratedFileFormat varchar(10) = null,
    @OutputL1CuratedFileWriteMode varchar(20) = null,
    
	--SQL
	@OutputDWStagingTable varchar(200) = null,
	@LookupColumns varchar(4000) = null,
    @OutputDWTable varchar(200) = null,
    @OutputDWTableWriteMode varchar(20) = null,
    @IngestCount int = null,

	--ADF Pipeline IDs
	@IngestADFPipelineRunID  uniqueidentifier = null




AS
BEGIN


DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

	--Check if Transformation records already exists for the input file for same transformation e.g it's a reload
		IF NOT EXISTS 
			(
				SELECT 1 
				FROM 
					[ELT].[L1TransformInstance]
				WHERE 
					[IngestID] = @IngestID
					AND L1TransformID = @L1TransformID
					AND InputRawFileSystem = @InputRawFileSystem
					AND InputRawFileFolder = @InputRawFileFolder
					AND InputRawFile = @InputRawFile
	
			)


	BEGIN
	--If this is a new transformation
		INSERT INTO [ELT].[L1TransformInstance]
			(
				[L1TransformID]
				,[IngestInstanceID]
				,[IngestID]
				,[NotebookName]
				,[NotebookPath]
				,[CustomParameters]
				,[InputRawFileSystem]
				,[InputRawFileFolder]
				,[InputRawFile]
				,[InputRawFileDelimiter]
				,[InputFileHeaderFlag]
				,[OutputL1CurateFileSystem]
				,[OutputL1CuratedFolder]
				,[OutputL1CuratedFile]
				,[OutputL1CuratedFileDelimiter]
				,[OutputL1CuratedFileFormat]
				,[OutputL1CuratedFileWriteMode]
				,[OutputDWStagingTable]
				,[LookupColumns]
				,[OutputDWTable]
				,[OutputDWTableWriteMode]
				,[RetryCount]
				,[ActiveFlag]
				,[IngestCount]
				,[IngestADFPipelineRunID]
				,[CreatedBy]
				,[CreatedTimestamp]
			   
			)
		VALUES
			(
				@L1TransformID
				,@IngestInstanceID
				,@IngestID
				,@NotebookName
				,@NotebookPath
				,@CustomParameters
				,@InputRawFileSystem
				,@InputRawFileFolder
				,@InputRawFile
				,@InputRawFileDelimiter
				,@InputFileHeaderFlag
				,@OutputL1CurateFileSystem
				,@OutputL1CuratedFolder
				,@OutputL1CuratedFile
				,@OutputL1CuratedFileDelimiter
				,@OutputL1CuratedFileFormat
				,@OutputL1CuratedFileWriteMode
				,@OutputDWStagingTable
				,@LookupColumns
				,@OutputDWTable
				,@OutputDWTableWriteMode
				,0
				,1
				,@IngestCount
				,@IngestADFPipelineRunID
				,SUSER_SNAME()
				,@localdate
		)
		END
	ELSE
		--If this is an existing Transformation
		BEGIN
			--Just update one record in case if there are duplicates
			UPDATE TOP (1) [ELT].[L1TransformInstance]
			SET 
				
				[IngestCount] = null
				,[L1TransformCount] = null
				,L1TransformStartTimestamp = null
				,[L1TransformEndTimestamp] = null
				,[L1TransformStatus] = null
				,[RetryCount] = 0
				,[ActiveFlag] = 1
				,[ReRunL1TransformFlag] = 1
				,[IngestADFPipelineRunID] = @IngestADFPipelineRunID
				,[L1TransformADFPipelineRunID] = null
				,[ModifiedBy] = suser_sname()
				,[ModifiedTimestamp] = @localdate
				
		WHERE 
			[IngestID] = @IngestID
			AND L1TransformID = @L1TransformID
			AND InputRawFileSystem = @InputRawFileSystem
			AND InputRawFileFolder = @InputRawFileFolder
			AND InputRawFile = @InputRawFile
		END
END



