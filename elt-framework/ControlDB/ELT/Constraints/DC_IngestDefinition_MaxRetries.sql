ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [DC_IngestDefinition_MaxRetries]
	DEFAULT 3
	FOR [MaxRetries]
