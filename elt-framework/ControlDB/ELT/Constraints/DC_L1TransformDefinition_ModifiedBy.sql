ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [DC_L1TransformDefinition_ModifiedBy]
	DEFAULT suser_sname()
	FOR [ModifiedBy]
