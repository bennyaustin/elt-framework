ALTER TABLE [ELT].[ColumnMapping]  
	ADD CONSTRAINT [FK_Attribute_IngestID] 
	FOREIGN KEY([IngestID])
	REFERENCES [ELT].[IngestDefinition] ([IngestID])