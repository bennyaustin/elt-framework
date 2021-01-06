ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [CC_L2TransformDefinition_CustomParameters]
	CHECK (ISJSON([CustomParameters]) = 1)