ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [DC_L1TransformDefinition_MaxRetries]
	DEFAULT 3
	FOR [MaxRetries]
