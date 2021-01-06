ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [DC_L2TransformDefinition_MaxRetries]
	DEFAULT 3
	FOR [MaxRetries]