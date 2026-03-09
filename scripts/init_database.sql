/* Running this script create the datawarehouse database after checking if it exists.
It also creates three schemas in the database: bronze,silver,gold

WARNING
and if it exists, it drops it and creates it afresh so before running the script backup
all the data to prevent data loss */


USE master;

--- Create new DataWarehouse
IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;
GO 

--- Creating schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
