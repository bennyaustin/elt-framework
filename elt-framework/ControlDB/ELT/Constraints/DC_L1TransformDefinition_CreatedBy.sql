ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [DC_L1TransformDefinition_CreatedBy]
	DEFAULT suser_sname()
	FOR [CreatedBy]