ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [CC_IngestDefinition_DataMapping]
	CHECK (ISJSON([DataMapping]) = 1)
