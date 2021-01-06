ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [CC_L2TransformDefinition_InputType]
	CHECK ([InputType] ='Raw' OR [InputType] ='Curated' OR [InputType] ='Datawarehouse')