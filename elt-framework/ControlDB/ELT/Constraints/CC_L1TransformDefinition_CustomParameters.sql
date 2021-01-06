ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [CC_L1TransformDefinition_CustomParameters]
	CHECK (ISJSON([CustomParameters]) = 1)