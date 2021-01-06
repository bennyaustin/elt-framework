ALTER TABLE [ELT].[L2TransformInstance]
	ADD CONSTRAINT [CC_L2TransformInstance_OutputL2CuratedFileWriteMode]
	CHECK ([OutputL2CuratedFileWriteMode]='append' OR [OutputL2CuratedFileWriteMode]= 'overwrite' OR [OutputL2CuratedFileWriteMode]= 'ignore' OR [OutputL2CuratedFileWriteMode]= 'error' OR [OutputL2CuratedFileWriteMode]= 'errorifexists')
