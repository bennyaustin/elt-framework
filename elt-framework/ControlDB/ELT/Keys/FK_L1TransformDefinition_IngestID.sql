ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [FK_L1TransformDefinition_IngestID]
	FOREIGN KEY ([IngestID])
	REFERENCES [ELT].[IngestDefinition] ([IngestID])