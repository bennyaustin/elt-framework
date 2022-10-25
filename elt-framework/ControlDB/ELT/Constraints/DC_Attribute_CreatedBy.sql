ALTER TABLE [ELT].[ColumnMapping] 
	ADD  CONSTRAINT [DC_Attribute_CreatedBy]  
	DEFAULT (suser_sname()) 
	FOR [CreatedBy]