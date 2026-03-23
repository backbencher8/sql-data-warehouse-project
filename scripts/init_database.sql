/* 
Create DB & Schemas

Script purpose:
  This script creates a new DB name "data_warehouse" after checking if it already exists. 
  If the DB exists, it is dropped & recreated. Additionally, the script sets up three schemas within DB: 'bronze', 'silver' &
  'gold'.

Warning:
  Running this script will drop the entire 'data_warehouse' database if exists.
  All data in the DB will be permanently deleted. Proceed with caution & ensure you have proper backups before running this script.
  */

-- Dropping the database if it already exists
DROP DATABASE IF EXISTS data_warehouse;

-- Creating the database
CREATE DATABASE data_warehouse;

-- Choosing the database
USE data_warehouse;

-- Creating the schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;




