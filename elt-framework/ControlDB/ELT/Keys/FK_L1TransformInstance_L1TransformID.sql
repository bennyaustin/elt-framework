ALTER TABLE [ELT].[L1TransformInstance]
	ADD CONSTRAINT [FK_L1TransformInstance_L1TransformID]
	FOREIGN KEY (L1TransformID)
	REFERENCES [ELT].[L1TransformDefinition] (L1TransformID)
