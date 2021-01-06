ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [CC_IngestDefinition_MaxIntervalNumber]
	CHECK ([MaxIntervalNumber] IS NULL OR  [MaxIntervalNumber] > 0)
