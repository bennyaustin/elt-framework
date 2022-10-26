ALTER TABLE [ELT].[ColumnMapping] 
	ADD CONSTRAINT [DC_Attribute_CreatedTimestamp]  
	DEFAULT (CONVERT([datetime],(CONVERT([datetimeoffset],getdate()) AT TIME ZONE 'AUS Eastern Standard Time'))) 
	FOR [CreatedTimestamp]