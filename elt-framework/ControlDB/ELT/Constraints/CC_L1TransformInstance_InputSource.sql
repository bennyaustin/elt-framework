ALTER TABLE [ELT].[L1TransformInstance]
ADD CONSTRAINT CC_L1TransformInstance_InputSource CHECK (
	([InputRawFileSystem] IS NOT NULL AND [InputRawFileFolder] IS NOT NULL AND [InputRawFile] IS NOT NULL AND [InputRawTable] IS NULL)
	OR
	([InputRawTable] IS NOT NULL AND [InputRawFileSystem] IS NULL AND [InputRawFileFolder] IS NULL AND [InputRawFile] IS NULL)
);