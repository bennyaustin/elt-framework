ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [CC_IngestDefinition_SourceStructure]
	CHECK (ISJSON([SourceStructure]) = 1)