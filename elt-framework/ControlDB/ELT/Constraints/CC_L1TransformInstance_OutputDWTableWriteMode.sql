ALTER TABLE [ELT].[L1TransformInstance]
	ADD CONSTRAINT [CC_L1TransformInstance_OutputDWTableWriteMode]
	CHECK ([OutputDWTableWriteMode]='append' OR [OutputDWTableWriteMode]='overwrite' OR [OutputDWTableWriteMode]='error' OR [OutputDWTableWriteMode]='errorifexists' OR [OutputDWTableWriteMode]='ignore')
