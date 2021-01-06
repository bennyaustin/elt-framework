ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [DC_L2TransformDefinition_ModifiedBy]
	DEFAULT suser_sname()
	FOR [ModifiedBy]
