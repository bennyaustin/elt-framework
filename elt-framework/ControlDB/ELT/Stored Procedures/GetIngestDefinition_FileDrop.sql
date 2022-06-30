CREATE PROCEDURE [ELT].[GetIngestDefinition_FileDrop]
	@SourceSystemName varchar(20),
	@StreamName VARCHAR(100),
	@SourceFolder VARCHAR(250), --where SourceFolder=Container/Folder
	@SourceFile VARCHAR(200)
AS
--/*
--Generic Stored Procedure to be used for File Drop Pipelines instead of the [ELT].[GetIngestDefinition] procedure which is relevant for database ingest pipelines
--*/
BEGIN
--For Testing


	DECLARE @IngestID INT
	SELECT @IngestID = [IngestID] FROM [ELT].[IngestDefinition] WHERE [SourceSystemName]=@SourceSystemName AND [StreamName]=@StreamName and [ActiveFlag]=1

	--If the Source File already exists (Reload)
	IF EXISTS ( SELECT 1 FROM [ELT].[IngestInstance] 
				WHERE (
						CASE 
							WHEN [SourceFileDropFileSystem] IS NULL 
								THEN [SourceFileDropFolder] --In most cases container will be null as it is part of folder name from event trigger
							WHEN [SourceFileDropFileSystem] IS NOT NULL AND LEN(RTRIM([SourceFileDropFolder])) =0 
								THEN [SourceFileDropFolder]
							ELSE [SourceFileDropFileSystem] +'/' + [SourceFileDropFolder]
					   END ) =@SourceFolder 
				AND [SourceFileDropFile]=@SourceFile)
		
BEGIN
		SELECT 
			top 1 
			II.[IngestID]
			, ID.[SourceSystemName]
			, ID.[StreamName]
			,ID.[Backend]
			,ID.[DataFormat]
			, II.[SourceFileDropFileSystem]
			, REPLACE(II.[SourceFileDropFolder],ID.[SourceFileDropFileSystem] +'/','') AS [SourceFileDropFolder] --Remove container name from folder path
			, II.[SourceFileDropFile]
			, ID.[SourceFileDelimiter]
			, ID.[SourceFileHeaderFlag]
			, II.[DestinationRawFileSystem]
			, II.[DestinationRawFolder]
			, II.[DestinationRawFile]
			, ID.[DataMapping]
			, CAST(1 AS BIT) AS [ReloadFlag]
			, ID.[L1TransformationReqdFlag]
			, ID.[L2TransformationReqdFlag]
			, ID.[DelayL1TransformationFlag]
			, ID.[DelayL2TransformationFlag]
			, II.[ADFIngestPipelineRunID]
			FROM 
				[ELT].[IngestInstance] AS II
					INNER JOIN [ELT].[IngestDefinition] AS ID
						ON II.[IngestID] =ID.[IngestID]
							AND II.[IngestID]=@IngestID
							AND (
								CASE 
									WHEN II.[SourceFileDropFileSystem] IS NULL 
										THEN II.[SourceFileDropFolder] --In most cases container will be null as it is part of folder name from event trigger
									WHEN II.[SourceFileDropFolder] IS NOT NULL AND LEN(RTRIM(II.[SourceFileDropFileSystem])) =0 
										THEN II.[SourceFileDropFolder] 
									ELSE II.[SourceFileDropFileSystem] +'/' + II.[SourceFileDropFolder]
								END ) = @SourceFolder 
							AND II.[SourceFileDropFile]=@SourceFile
			ORDER BY [IngestInstanceID] DESC
		END
	ELSE
	--If this is a new file
		BEGIN

		DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

		SELECT 
			[IngestID]
			, [SourceSystemName]
			, [StreamName]		
			,[Backend]
			,[DataFormat]
			, [SourceFileDropFileSystem]
			, REPLACE(@SourceFolder,[SourceFileDropFileSystem] +'/','')  AS [SourceFileDropFolder] --Remove container name from folder path
			, @SourceFile AS [SourceFileDropFile]
			, [SourceFileDelimiter]
			, [SourceFileHeaderFlag]
			, [DestinationRawFileSystem]
			, [DestinationRawFolder] = 
				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([DestinationRawFolder] COLLATE SQL_Latin1_General_CP1_CS_AS
				,'YYYY',CAST(Year(@localdate) as varchar(4)))
				,'MM',Right('0'+ CAST(Month(@localdate) AS varchar(2)),2))
				,'DD',Right('0'+Cast(Day(@localdate) as varchar(2)),2))
				,'HH',Right('0'+ CAST(DatePart(hh,@localdate) as varchar(2)),2))
				,'MI',Right('0'+ CAST(DatePart(mi,@localdate) as varchar(2)),2))
				,'SS',Right('0'+ CAST(DatePart(ss,@localdate) as varchar(2)),2))
			
			, [DestinationRawFile] = 
				REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE([DestinationRawFile] COLLATE SQL_Latin1_General_CP1_CS_AS
				,'YYYY',CAST(Year(@localdate) AS varchar(4)))
				,'MM',Right('0'+ CAST(Month(@localdate) AS varchar(2)),2))
				,'DD',Right('0'+Cast(Day(@localdate) as varchar(2)),2))
				,'HH',Right('0'+ CAST(DatePart(hh,@localdate) AS varchar(2)),2))
				,'MI',Right('0'+ CAST(DatePart(mi,@localdate) AS varchar(2)),2))
				,'SS',Right('0'+ CAST(DatePart(ss,@localdate) AS varchar(2)),2))
			, [DataMapping]
			, CAST(0 AS BIT) AS [ReloadFlag]
			, [L1TransformationReqdFlag]
			, [L2TransformationReqdFlag]
			, [DelayL1TransformationFlag]
			, [DelayL2TransformationFlag]
			, NULL AS [ADFPipelineRunID]
			
			FROM 
				[ELT].[IngestDefinition]
			WHERE 
				[IngestID]=@IngestID
		END
END
