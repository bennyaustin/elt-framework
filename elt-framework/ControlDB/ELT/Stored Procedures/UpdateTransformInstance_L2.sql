CREATE PROCEDURE [ELT].[UpdateTransformInstance_L2]
	@L2TransformInstanceId INT
   ,@L2TransformStatus VARCHAR(20)
   ,@L2TransformADFPipelineRunID UNIQUEIDENTIFIER
   ,@InputCount INT = NULL
   ,@L2TransformCount INT = NULL
   ,@DataFromTimestamp Datetime2 = null
   ,@DataToTimestamp Datetime2 = null
   ,@DataFromNumber int = null
   ,@DataToNumber int = null
   ,@MaxRetries int = null
AS
BEGIN

	DECLARE @localdate datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

		Update 
			[ELT].[L2TransformInstance]
		SET 
			[L2TransformStartTimestamp] = CASE 
											WHEN @L2TransformStatus IN ('Running') 
												THEN @localdate 
											ELSE [L2TransformStartTimestamp] 
										END

			, [L2TransformEndTimestamp] = CASE 
												WHEN @L2TransformStatus IN ('Success','Failure','ReRunSuccess','ReRunFailure') 
													THEN @localdate 
												ELSE NULL 
											END
			, [DataFromTimestamp] = @DataFromTimestamp
			, [DataToTimestamp] = @DataToTimestamp	
			, [DataFromNumber] = @DataFromNumber
			, [DataToNumber] = @DataToNumber
			, [L2TransformStatus] = CASE 
										WHEN ([ReRunL2TransformFlag] = 1 OR [RetryCount] > 0) AND @L2TransformStatus = 'Success' 
											THEN 'ReRunSuccess'
										WHEN [ReRunL2TransformFlag] = 1 AND @L2TransformStatus = 'Failure' 
											THEN 'ReRunFailure'
									ELSE @L2TransformStatus 
								 END
			, [ActiveFlag] = CASE 
								WHEN @L2TransformStatus IN ('Success','ReRunSuccess') 
									THEN 0
								WHEN @L2TransformStatus = 'Failure' and ISNULL(RetryCount,0) +1 >= @MaxRetries
									THEN 0
								ELSE 1 
							END
			, [ReRunL2TransformFlag] = CASE 
											WHEN [ReRunL2TransformFlag] =1 AND @L2TransformStatus IN ('Success','ReRunSuccess') 
												THEN 0
											WHEN @L2TransformStatus in ('Failure', 'ReRunFailure') and ISNULL(RetryCount,0) +1 >= @MaxRetries
													THEN 0
											ELSE [ReRunL2TransformFlag] 
										END
			, [RetryCount] = CASE
								WHEN
									@L2TransformStatus in ('Success', 'ReRunSuccess')
										THEN 0
								WHEN	
									@L2TransformStatus in ('Failure', 'ReRunFailure')
										THEN ISNULL([RetryCount],0) + 1
								ELSE [RetryCount]
								END
							
			, [ModifiedBy] =suser_sname()
			, [ModifiedTimestamp]=@localdate
			, [L2TransformADFPipelineRunID] = @L2TransformADFPipelineRunID
			, [InputCount] = ISNULL(@InputCount,[InputCount])
			, [L2TransformCount] = ISNULL(@L2TransformCount,[L2TransformCount])
		WHERE 
			[L2TransformInstanceID] = @L2TransformInstanceId
END