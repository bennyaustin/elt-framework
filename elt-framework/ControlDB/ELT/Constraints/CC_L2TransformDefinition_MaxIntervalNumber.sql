ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [CC_L2TransformDefinition_MaxIntervalNumber]
	CHECK ([MaxIntervalNumber] IS NULL OR  [MaxIntervalNumber] > 0)
