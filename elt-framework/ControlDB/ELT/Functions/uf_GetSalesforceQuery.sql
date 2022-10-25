CREATE FUNCTION [ELT].[uf_GetSalesforceQuery]
(
	@QueryType varchar(20), --SourceQuery|StatQuery
	@IngestID INT,
	@EntityName varchar(100),
	@DeltaName varchar(20),
	@FromDate datetime2=NULL,
	@ToDate datetime2=NULL,
	@MaxIntervalMinutes int=NULL
	)
RETURNS varchar(MAX)
	AS
BEGIN
	Declare @Query varchar(MAX)
	DECLARE @MappingExists INT
	DECLARE @Columns NVARCHAR(MAX)
	Declare @From datetime2
	DECLARE @FromStr varchar(27), @ToStr varchar(27)

	SET @From = (CASE 
					WHEN @FromDate is NULL THEN '1900-01-01 00:00:00'
					ELSE @FromDate
				END)

	--Datetime Strings
	SET @ToStr = FORMAT(CAST((
								CASE 
									WHEN @ToDate IS NOT NULL 
										THEN @ToDate
								WHEN @ToDate is NULL AND @MaxIntervalMinutes IS NULL 
									THEN GETDATE()
								WHEN @ToDate is NULL AND @MaxIntervalMinutes IS NOT NULL AND DATEADD(DAY,@MaxIntervalMinutes/1440,@From) > GETDATE() 
									THEN GETDATE()
								WHEN @ToDate is NULL AND @MaxIntervalMinutes IS NOT NULL AND DATEADD(DAY,@MaxIntervalMinutes/1440,@From) <= GETDATE() 
									THEN DATEADD(DAY,@MaxIntervalMinutes/1440,@From)
								ELSE GETDATE()
								END
							) 
								as datetime), 'yyyy-MM-ddTHH:mm:ssZ')

	SET @FromStr = (CASE 
					WHEN @FromDate is NULL THEN '1900-01-01T00:00:00Z'
					ELSE FORMAT(CAST(@FromDate as datetime), 'yyyy-MM-ddTHH:mm:ssZ')
				END)

	--Set Columns
	SET @Columns = (		
						SELECT
							DISTINCT
								STUFF((
									SELECT 
										', ' + SourceName
								    FROM     
										[ELT].[ColumnMapping]
									WHERE IngestID = @IngestID
										AND ActiveFlag = 1
									ORDER BY TargetOrdinalPosition ASC
								       FOR XML PATH('')
								       ),1,1,'') AS ColumnList
						FROM
							[ELT].[ColumnMapping]
						WHERE IngestID = @IngestID
							and ActiveFlag = 1 
						GROUP BY SourceName
					)

	--SourceQuery
	IF @QueryType ='SourceQuery'
		BEGIN
			SET @Query = 
						'SELECT ' + COALESCE(@Columns, ' * ')
						+ ' FROM ' + @EntityName 
						+ CASE 
							WHEN @DeltaName IS NOT NULL
								THEN ' WHERE ' 	+ @DeltaName + ' > ' + @FromStr + ' AND ' + @DeltaName + ' <= ' + @ToStr
							ELSE ''
						END
		END

	--StatQuery
	IF @QueryType ='StatQuery'
		BEGIN
			SET @Query = CASE 
							WHEN @DeltaName IS NOT NULL 
								THEN 'SELECT MIN('+@DeltaName+') AS DataFromTimestamp,' + ' MAX('+@DeltaName+') AS DataToTimestamp,'+ 'COUNT(*) AS SourceCount' 
							WHEN @DeltaName IS NULL
								THEN 'SELECT ''1900-01-01T00:00:00Z'' AS DataFromTimestamp, '''+FORMAT(CAST(GETDATE() as datetime), 'yyyy-MM-ddTHH:mm:ssZ')+''' AS DataToTimestamp,  COUNT(*) AS SourceCount '
							END
							+ ' FROM ' + @EntityName 
							+ CASE 
								WHEN @DeltaName IS NOT NULL
									THEN ' WHERE ' + @DeltaName + ' > ' + '''' + FORMAT(CAST(@FromStr as datetime), 'yyyy-MM-dd HH:mm:ss') + '''' +  ' AND ' + @DeltaName + ' <= ' + '''' + FORMAT(CAST(@ToStr as datetime), 'yyyy-MM-dd HH:mm:ss') + ''''
								ELSE ''
							END
						END
				
 -- Return the result of the function
	Return @Query

END


GO


