/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/

--:r .\Script1.PostDeployment.sql
--IngestDefinition
:r .\datasources\RestAPI\IngestDefinition\PurviewRestAPI.sql
:r .\datasources\RestAPI\IngestDefinition\AzureRestAPI.sql

--L1TransformDefinition
:r .\datasources\RestAPI\L1TransformDefinition\L1T_PurviewRestAPI.sql