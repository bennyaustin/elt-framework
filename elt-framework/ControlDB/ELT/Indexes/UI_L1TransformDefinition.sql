CREATE UNIQUE INDEX [UI_L1TransformDefinition]
	ON [ELT].[L1TransformDefinition]
	([InputRawFileSystem],[InputRawFileFolder],[InputRawFile],[InputRawTable],[OutputL1CurateFileSystem],[OutputL1CuratedFolder],[OutputL1CuratedFile],[OutputDWTable])
