ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [DC_L2TransformDefinition_RunSequence]
	DEFAULT 100 
	FOR [RunSequence]