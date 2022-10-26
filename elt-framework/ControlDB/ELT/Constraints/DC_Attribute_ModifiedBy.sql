ALTER TABLE [ELT].[ColumnMapping] 
	ADD CONSTRAINT [DC_Attribute_ModifiedBy]  
	DEFAULT (suser_sname()) 
	FOR [ModifiedBy]