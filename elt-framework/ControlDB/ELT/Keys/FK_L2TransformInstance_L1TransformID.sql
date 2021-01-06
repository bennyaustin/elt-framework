ALTER TABLE [ELT].[L2TransformInstance]
	ADD CONSTRAINT [FK_L2TransformInstance_L1TransformID]
	FOREIGN KEY (L1TransformID)
	REFERENCES [ELT].[L1TransformDefinition] (L1TransformID)
