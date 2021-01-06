/*
 Pre-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be executed before the build script.	
 Use SQLCMD syntax to include a file in the pre-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the pre-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/
----Create database user for Azure Datafactory Managed Identity
--CREATE USER [$(adf_name)] FROM EXTERNAL PROVIDER;

----Assign Roles to ADF Managed Identity
--EXEC sp_addrolemember db_datareader, [$(adf_name)]; --Reader
--EXEC sp_addrolemember db_datawriter, [$(adf_name)]; --Writer
--GRANT EXECUTE TO [$(adf_name)]; --Execute Stored Procedures
