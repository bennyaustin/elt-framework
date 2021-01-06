ALTER TABLE [ELT].[L2TransformInstance]
	ADD CONSTRAINT [CC_L2TransformInstance_OutputDWTableWriteMode]
	CHECK ([OutputDWTableWriteMode]='append' OR [OutputDWTableWriteMode]= 'overwrite' OR [OutputDWTableWriteMode]= 'ignore' OR [OutputDWTableWriteMode]= 'error' OR [OutputDWTableWriteMode]= 'errorifexists')
