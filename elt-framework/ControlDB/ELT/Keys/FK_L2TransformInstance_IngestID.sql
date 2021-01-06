ALTER TABLE [ELT].[L2TransformInstance]
	ADD CONSTRAINT [FK_L2TransformInstance_IngestID]
	FOREIGN KEY (IngestID)
	REFERENCES [ELT].[IngestDefinition] (IngestID)
