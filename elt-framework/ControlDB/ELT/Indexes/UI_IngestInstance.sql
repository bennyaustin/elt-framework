CREATE UNIQUE INDEX [UI_IngestInstance]
	ON  [ELT].[IngestInstance]
	([DestinationRawFileSystem],[DestinationRawFolder],[DestinationRawFile],[DestinationRawTable])
