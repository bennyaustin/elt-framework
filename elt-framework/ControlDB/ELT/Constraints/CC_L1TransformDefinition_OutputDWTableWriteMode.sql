ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [CC_L1TransformDefinition_OutputDWTableWriteMode]
CHECK ([OutputDWTableWriteMode]='append' OR [OutputDWTableWriteMode]='overwrite' OR [OutputDWTableWriteMode]='error' OR [OutputDWTableWriteMode]='errorifexists' OR [OutputDWTableWriteMode]='ignore')
