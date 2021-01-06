CREATE FUNCTION [ELT].[uf_GetTabularTranslatorMappingJson]
(
	@DataMappingJson VARCHAR(MAX)
)
RETURNS VARCHAR(MAX)
AS
BEGIN
	-- Uses standard ADF Explicit Schema Mapping
	-- https://docs.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping
	DECLARE @TabularTranslatorJson VARCHAR(MAX) = '{
		"type": "TabularTranslator",
		"mappings": <MAPPINGS>
	}'
	RETURN REPLACE(@TabularTranslatorJson, '<MAPPINGS>', @DataMappingJson)
END
