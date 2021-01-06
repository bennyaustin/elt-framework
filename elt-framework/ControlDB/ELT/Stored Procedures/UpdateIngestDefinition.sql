CREATE PROCEDURE [ELT].[UpdateIngestDefinition]
	@IngestID INT,
	@LastDeltaDate Datetime2=null,
	@LastDeltaNumer int=null,
	@IngestStatus varchar(20),
	@ReloadFlag bit=0
AS
BEGIN

		DECLARE @localdate as datetime	= CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')

		Update 
			[ELT].[IngestDefinition]
		SET 
			[LastDeltaDate] =
							CASE
								--When Successful and the DataToDate does not move forward since LastDeltaDate, Increase LastDeltaDate by the Interval
								WHEN @LastDeltaDate IS NOT NULL AND @ReloadFlag <> 1 AND @IngestStatus IN ('Success','ReRunSuccess') AND @LastDeltaDate = [LastDeltaDate] 
									and [MaxIntervalMinutes] is NOT NULL
									THEN 
										CASE 
											WHEN 
												DateAdd(minute,[MaxIntervalMinutes],@LastDeltaDate) > ELT.[uf_GetAestDateTime]()
													THEN CONVERT(VARCHAR(30),ELT.[uf_GetAestDateTime](),120)
											ELSE
												DateAdd(minute,[MaxIntervalMinutes],[LastDeltaDate])
										END
								--Re-run delta date is later than existing delta date
								WHEN @LastDeltaDate IS NOT NULL AND @IngestStatus IN ('Success','ReRunSuccess') AND datediff_big(ss,[LastDeltaDate],@LastDeltaDate) >= 0 
									THEN @LastDeltaDate
								--Re-run delta date is earlier than existing delta date
								WHEN @LastDeltaDate IS NOT NULL AND @IngestStatus IN ('Success','ReRunSuccess')  AND datediff_big(ss,@LastDeltaDate,[LastDeltaDate]) >=0 
									THEN [LastDeltaDate]
								ELSE [LastDeltaDate]
							END
			, [LastDeltaNumber] = 
							CASE
								WHEN @LastDeltaNumer IS NOT NULL AND @IngestStatus IN ('Success','ReRunSuccess') 
									THEN @LastDeltaNumer
								WHEN @LastDeltaNumer IS NOT NULL AND @IngestStatus IN ('Failure','ReRunFailure') 
									THEN [LastDeltaNumber]
								WHEN @LastDeltaNumer IS NULL AND @ReloadFlag <> 1 AND @IngestStatus IN ('Success','ReRunSuccess')  
									THEN ([LastDeltaNumber] + [MaxIntervalNumber])
								ELSE [LastDeltaNumber]
							END
			,[ModifiedBy] =suser_sname()
			, [ModifiedTimestamp]=@localdate
	WHERE [IngestID]=@IngestID
END