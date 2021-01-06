ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [DC_IngestDefinition_CreatedTimestamp]
	DEFAULT  CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
	FOR [CreatedTimestamp]
