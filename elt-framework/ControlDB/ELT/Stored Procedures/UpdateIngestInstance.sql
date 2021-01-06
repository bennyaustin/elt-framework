CREATE PROCEDURE [ELT].[UpdateIngestInstance]
	@ADFIngestPipelineRunID Uniqueidentifier,
	@IngestStatus varchar(20) =null,
	@DataFromTimestamp Datetime2 =null,
	@DataToTimestamp Datetime2 =null,
	@DataFromNumber int =null,
	@DataToNumber int =null,
	@SourceCount int=null,
	@IngestCount int=null,
	@ReloadFlag bit
AS
BEGIN

		DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

	Update 
		[ELT].[IngestInstance]
	SET 
		[DataFromTimestamp] = @DataFromTimestamp
		,[DataToTimestamp]=@DataToTimestamp
		,[DataFromNumber] = @DataFromNumber
		,[DataToNumber]=@DataToNumber
		,[SourceCount] =@SourceCount
		,[IngestCount]=@IngestCount
		,[IngestEndTimestamp] =@localdate
		,[IngestStatus] =(CASE WHEN @ReloadFlag=1 AND @IngestStatus='Success' THEN 'ReRunSuccess'
							WHEN   @ReloadFlag=1 AND @IngestStatus='Failure' THEN 'ReRunFailure'
							ELSE @IngestStatus
						END)
		,[RetryCount] = (CASE WHEN @IngestStatus  IN ('Success','ReRunSuccess') THEN 0
							WHEN @IngestStatus IN ('Failure','ReRunFailure')  THEN ISNULL([RetryCount],0) +1
						END)
		,[ReloadFlag] =(CASE WHEN @ReloadFlag=1 AND @IngestStatus IN ('Success','ReRunSuccess') THEN 0
							WHEN @ReloadFlag=1 AND @IngestStatus IN ('Failure','ReRunFailure') THEN 1
							ELSE 0
						 END)
		,[ModifiedBy]=suser_sname()
		,[ModifiedTimestamp] = @localdate
	WHERE
		ADFIngestPipelineRunID =@ADFIngestPipelineRunID
END
