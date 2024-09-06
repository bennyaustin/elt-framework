CREATE PROCEDURE [ELT].[UpdateTransformInstance_L1]
	@L1TransformInstanceId INT
   , @L1TransformStatus VARCHAR(20)
   , @L1TransformADFPipelineRunID UNIQUEIDENTIFIER
   , @IngestCount INT = NULL
   , @L1TransformInsertCount INT = NULL
   , @L1TransformUpdateCount INT = NULL
   , @L1TransformDeleteCount INT = NULL
   , @MaxRetries int = null
AS
BEGIN
	
	DECLARE @localdate datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

		Update 
			[ELT].[L1TransformInstance]
		SET 
			[L1TransformStartTimestamp] = CASE 
											WHEN @L1TransformStatus IN ('Running') 
												THEN @localdate 
											ELSE [L1TransformStartTimestamp] 
										END

			, [L1TransformEndTimestamp] = CASE
												WHEN @L1TransformStatus IN ('Success','Failure','ReRunSuccess','ReRunFailure') 
													THEN @localdate 
												ELSE NULL 
											END
										
			, [L1TransformStatus] = CASE 
										WHEN ([ReRunL1TransformFlag] = 1 OR [RetryCount] >0) AND @L1TransformStatus='Success' 
											THEN 'ReRunSuccess'
										WHEN [ReRunL1TransformFlag] = 1 AND @L1TransformStatus='Failure' 
											THEN 'ReRunFailure'
									ELSE @L1TransformStatus 
								 END
			, [ActiveFlag] = CASE
								WHEN @L1TransformStatus IN ('Success','ReRunSuccess') 
									THEN 0
								WHEN @L1TransformStatus = 'Failure' and ISNULL(RetryCount,0) +1 >= @MaxRetries
									THEN 0
								ELSE 1 
							END
			, [ReRunL1TransformFlag] =	CASE 
											WHEN [ReRunL1TransformFlag] = 1 AND @L1TransformStatus IN ('Success','ReRunSuccess') 
												THEN 0
											WHEN @L1TransformStatus in ('Failure', 'ReRunFailure') and ISNULL(RetryCount,0) +1 >= @MaxRetries
													THEN 0
											ELSE [ReRunL1TransformFlag] 
										END
			, [RetryCount] = CASE
								WHEN
									@L1TransformStatus in ('Success', 'ReRunSuccess')
										THEN 0
								WHEN	
									@L1TransformStatus in ('Failure', 'ReRunFailure')
										THEN ISNULL([RetryCount],0) + 1
								Else [RetryCount]
								END
							
			, [ModifiedBy] =suser_sname()
			, [ModifiedTimestamp]=@localdate
			, [L1TransformADFPipelineRunID] = @L1TransformADFPipelineRunID
			, [IngestCount] = ISNULL(@IngestCount,[IngestCount])
			, [L1TransformInsertCount] = ISNULL(@L1TransformInsertCount,[L1TransformInsertCount])
			, [L1TransformUpdateCount] = ISNULL(@L1TransformUpdateCount,[L1TransformUpdateCount])
			, [L1TransformDeleteCount] = ISNULL(@L1TransformDeleteCount,[L1TransformDeleteCount])
	WHERE 
		[L1TransformInstanceID] = @L1TransformInstanceId
END
