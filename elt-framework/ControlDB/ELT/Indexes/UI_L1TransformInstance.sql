CREATE UNIQUE INDEX [UI_L1TransformInstance]
	ON [ELT].[L1TransformInstance]
	([InputRawFileSystem],[InputRawFileFolder],[InputRawFile],[InputRawTable],[DataFromTimestamp],[DataToTimestamp],[OutputL1CurateFileSystem],[OutputL1CuratedFolder],[OutputL1CuratedFile],[OutputDWTable])