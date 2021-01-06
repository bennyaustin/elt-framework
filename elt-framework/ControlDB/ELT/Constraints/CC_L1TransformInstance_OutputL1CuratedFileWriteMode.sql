ALTER TABLE [ELT].[L1TransformInstance]
	ADD CONSTRAINT [CC_L1TransformInstance_OutputL1CuratedFileWriteMode]
	CHECK ([OutputL1CuratedFileWriteMode] ='append' OR [OutputL1CuratedFileWriteMode] ='overwrite' OR [OutputL1CuratedFileWriteMode] ='ignore'  OR [OutputL1CuratedFileWriteMode] = 'error' OR [OutputL1CuratedFileWriteMode]='errorifexists')
