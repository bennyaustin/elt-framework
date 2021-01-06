ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [DC_L2TransformDefinition_CreatedBy]
	DEFAULT suser_sname()
	FOR [CreatedBy]
