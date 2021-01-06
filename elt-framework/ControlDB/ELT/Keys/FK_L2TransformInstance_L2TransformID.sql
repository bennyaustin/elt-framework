ALTER TABLE [ELT].[L2TransformInstance]
	ADD CONSTRAINT [FK_L2TransformInstance_L2TransformID]
	FOREIGN KEY (L2TransformID)
	REFERENCES [ELT].[L2TransformDefinition] (L2TransformID)
