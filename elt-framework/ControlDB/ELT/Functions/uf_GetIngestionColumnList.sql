CREATE FUNCTION [ELT].[uf_GetIngestionColumnList]
(
	@IngestID INT
)
	
RETURNS varchar(max)
AS BEGIN
	DECLARE @Columns NVARCHAR(MAX)

	--Columns
	SET @Columns = (
					SELECT
						DISTINCT 
						stuff((
							SELECT ', ' + SourceName + ' as ' + TargetName
						      FROM     
								[ELT].[ColumnMapping]
							WHERE IngestID = @IngestID
							and ActiveFlag = 1
							ORDER BY TargetOrdinalPosition ASC
						       FOR XML PATH('')
						       ),1,1,'') as Columns
						FROM [ELT].[ColumnMapping]
						WHERE IngestID = @IngestID
						and ActiveFlag = 1
						GROUP BY SourceName
					)
	SET @Columns = COALESCE(@Columns, ' * ')
				
 -- Return the columns of the function
	RETURN @Columns
END
GO


