CREATE PROCEDURE [ELT].[InsertIngestInstance]
	@IngestID INT 
	,@SourceFileDropFileSystem varchar(50)=null 
	,@SourceFileDropFolder varchar(200)=null
	,@SourceFileDropFile varchar(200)=null
	,@DestinationRawFileSystem varchar(50)=null 
	,@DestinationRawFolder varchar(200)=null
	,@DestinationRawFile varchar(200)=null
	,@DestinationRawTable varchar(200)=null
	,@ReloadFlag bit =0
	,@ADFPipelineRunID UniqueIdentifier=null
AS

BEGIN

DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')


	--NOTE:Potential enhancements for lookup.
	-- 1. Job to Purge the Instance Tables to Blob after 3 months
	-- 2. Use the AdfPipeline Instance Run ID for the lookup, though this needs to be the individual. Lookup Including Index on the lookup columns.
	-- 3. Create a Hash Column on DestinationFileSystem/Folder/File and use as the lookup. Including Index on the lookup columns.

	--Normal Run
	IF (@ReloadFlag=0 AND NOT EXISTS (
										SELECT 1 
										FROM [ELT].[IngestInstance] 
										WHERE (@DestinationRawFileSystem IS NULL OR [DestinationRawFileSystem] = @DestinationRawFileSystem)
										AND (@DestinationRawFolder IS NULL OR [DestinationRawFolder] = @DestinationRawFolder)
										AND (@DestinationRawFile IS NULL OR [DestinationRawFile] = @DestinationRawFile)
										AND (@DestinationRawTable IS NULL OR [DestinationRawTable] = @DestinationRawTable)
									)
		)
	BEGIN
		INSERT INTO [ELT].[IngestInstance]
				   ([IngestID]
				   ,[SourceFileDropFileSystem]
				   ,[SourceFileDropFolder]
				   ,[SourceFileDropFile]
				   ,[DestinationRawFileSystem]
				   ,[DestinationRawFolder]
				   ,[DestinationRawFile]
				   ,[DestinationRawTable]
				   ,[IngestStartTimestamp]
				   ,[IngestEndTimestamp]
				   ,[IngestStatus]
				   ,[RetryCount]
				   ,[ReloadFlag]
				   ,[CreatedBy]
				   ,[CreatedTimestamp]
				   ,[ModifiedBy]
				   ,[ModifiedTimestamp]
				   ,ADFIngestPipelineRunID)
			 VALUES
				   (@IngestID
				   ,@SourceFileDropFileSystem
				   ,@SourceFileDropFolder
				   ,@SourceFileDropFile
				   ,@DestinationRawFileSystem
				   ,@DestinationRawFolder
				   ,@DestinationRawFile
				   ,@DestinationRawTable
				   ,@localdate
				   ,NULL
				   ,'Running'
				   ,0
                   ,0
				   ,suser_sname()
				   ,@localdate
				   ,NULL
				   ,NULL
				   ,@ADFPipelineRunID)
	END

	--Re-load
	IF (@ReloadFlag=1 OR EXISTS (
									SELECT 1 FROM [ELT].[IngestInstance] 
									WHERE ( @DestinationRawFileSystem IS NULL OR [DestinationRawFileSystem] = @DestinationRawFileSystem)
										AND (@DestinationRawFolder IS NULL OR [DestinationRawFolder] = @DestinationRawFolder)
										AND (@DestinationRawFile IS NULL OR [DestinationRawFile] = @DestinationRawFile)
										AND (@DestinationRawTable IS NULL OR [DestinationRawTable] = @DestinationRawTable)
								)
		)
	BEGIN
		Update [ELT].[IngestInstance]
		SET [IngestStartTimestamp] = @localdate
			,[IngestEndTimestamp] = NULL
			,[SourceCount]=NULL
			,[IngestCount]=NULL
			,[IngestStatus]='Running'
			,[ModifiedBy]=suser_sname()
			,[ModifiedTimestamp] = @localdate
			,ADFIngestPipelineRunID = @ADFPipelineRunID
		--Unique Keys
		WHERE (@DestinationRawFileSystem IS NULL OR [DestinationRawFileSystem] = @DestinationRawFileSystem)
			AND (@DestinationRawFolder IS NULL OR [DestinationRawFolder] = @DestinationRawFolder)
			AND (@DestinationRawFile IS NULL OR [DestinationRawFile] = @DestinationRawFile)
			AND (@DestinationRawTable IS NULL OR [DestinationRawTable] = @DestinationRawTable)
						
	END
END

