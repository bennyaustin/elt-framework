ALTER TABLE [ELT].[L2TransformDefinition]
	ADD CONSTRAINT [DC_L2TransformDefinition_ModifiedTimestamp]
	DEFAULT CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
	FOR [ModifiedTimestamp]
