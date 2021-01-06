ALTER TABLE [ELT].[IngestDefinition]
	ADD CONSTRAINT [DC_IngestDefinition_ModifiedBy]
	DEFAULT suser_sname()
	FOR [ModifiedBy]
