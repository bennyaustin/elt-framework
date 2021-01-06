CREATE UNIQUE INDEX [UI_IngestDefinition]
	ON [ELT].[IngestDefinition]
	([SourceSystemName],[StreamName])
