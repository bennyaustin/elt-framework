ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [CC_L2TransformDefinition_MaxIntervalMinutes]
	CHECK ([MaxIntervalMinutes] IS NULL OR  [MaxIntervalMinutes] > 0)
