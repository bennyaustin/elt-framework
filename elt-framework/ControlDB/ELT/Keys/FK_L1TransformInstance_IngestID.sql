ALTER TABLE [ELT].[L1TransformInstance]
	ADD CONSTRAINT [FK_L1TransformInstance_IngestID]
	FOREIGN KEY ([IngestID])
	REFERENCES [ELT].[IngestDefinition] ([IngestID])
