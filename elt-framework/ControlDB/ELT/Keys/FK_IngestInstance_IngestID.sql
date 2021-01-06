ALTER TABLE [ELT].[IngestInstance]
	ADD CONSTRAINT [FK_IngestInstance_IngestID]
	FOREIGN KEY (IngestID)
	REFERENCES [ELT].[IngestDefinition] (IngestID)
