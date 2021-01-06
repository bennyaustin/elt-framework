CREATE UNIQUE INDEX [UI_L1TransformInstance]
	ON [ELT].[L1TransformInstance]
	([InputRawFileSystem],[InputRawFileFolder],[InputRawFile],[OutputL1CurateFileSystem],[OutputL1CuratedFolder],[OutputL1CuratedFile])