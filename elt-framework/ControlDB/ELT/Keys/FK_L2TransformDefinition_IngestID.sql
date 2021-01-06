ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [FK_L2TransformDefinition_IngestID]
	FOREIGN KEY (IngestID)
	REFERENCES [ELT].[IngestDefinition] (IngestID)
