ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [CC_L2TransformDefinition_OutputL2CuratedFileWriteMode]
	CHECK ([OutputL2CuratedFileWriteMode]='append' OR [OutputL2CuratedFileWriteMode]='overwrite' OR [OutputL2CuratedFileWriteMode]='ignore' OR [OutputL2CuratedFileWriteMode]='error' OR [OutputL2CuratedFileWriteMode]='errorifexists')
