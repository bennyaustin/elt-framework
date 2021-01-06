CREATE PROCEDURE [ELT].[UpdateTransformDefinition_L2]
	@L2TransformID INT,
	@LastDeltaDate Datetime2 =null,
	@LastDeltaNumber int =null
	as
BEGIN
	Update [ELT].[L2TransformDefinition]
	SET 
		[ModifiedBy] =suser_sname(),
		[ModifiedTimestamp]=GETDATE(),
		[LastDeltaDate] = COALESCE(@LastDeltaDate,[LastDeltaDate],ELT.uf_GetAestDateTime()),
		[LastDeltaNumber] = COALESCE(@LastDeltaNumber,[LastDeltaNumber])

	WHERE [L2TransformID] = @L2TransformID
END
GO