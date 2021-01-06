ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [CC_IngestDefinition_MaxIntervalMinutes]
	CHECK ([MaxIntervalMinutes] IS NULL OR  [MaxIntervalMinutes] > 0)
