/*
==============================================================================
Create DataBase and Schemas 
==============================================================================
Script purpose:
  This script creates a new database named 'DataWareHouse' after checking if it already exsists.
  If the Database exsists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'bronze', 'silver', 'gold'.

Warning:
  Running this script will drop the entire 'DataWareHouse' database if it exsists.
  All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups before running this script. */

Use master;
Go

-- Drop and recreate the 'DateWareHouse' database
if exists (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database 
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create schemas
create schema bronze;
go

create schema silver;
go

create schema gold;
go
