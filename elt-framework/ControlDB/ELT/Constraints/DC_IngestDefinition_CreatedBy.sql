ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [DC_IngestDefinition_CreatedBy]
	DEFAULT suser_sname()
	FOR [CreatedBy]