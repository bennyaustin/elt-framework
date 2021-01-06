ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [CC_L2TransformDefinition_OutputDWTableWriteMode]
	CHECK ([OutputDWTableWriteMode]='append' OR [OutputDWTableWriteMode]='overwrite' OR [OutputDWTableWriteMode]='ignore' OR [OutputDWTableWriteMode]='error' OR [OutputDWTableWriteMode]='errorifexists')
