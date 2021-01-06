CREATE UNIQUE INDEX [UI_L2TransformDefinition]
	ON [ELT].[L2TransformDefinition]
	([InputFileSystem],[InputFileFolder],[InputFile],[InputDWTable],[OutputL2CurateFileSystem],[OutputL2CuratedFolder],[OutputL2CuratedFile])
