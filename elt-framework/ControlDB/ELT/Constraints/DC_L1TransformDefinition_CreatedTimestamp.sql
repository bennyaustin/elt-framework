ALTER TABLE [ELT].[L1TransformDefinition]
	ADD CONSTRAINT [DC_L1TransformDefinition_CreatedTimestamp]
	DEFAULT CONVERT(datetime,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
	FOR [CreatedTimestamp]
