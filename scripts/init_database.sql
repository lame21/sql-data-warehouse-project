=========================================================
 Title: Creating Database and Schemas
 Description:
   This script creates a database named 'DataWarehouse'
   (if it does not already exist), switches to it, and
   sets up the standard schema layers for a data warehouse:
     - bronze : Raw data storage layer
     - silver : Cleaned and transformed layer
     - gold   : Curated and analytics-ready layer
=========================================================


-- Check if the database 'DataWarehouse' exists, create it if not
CREATE DATABASE IF NOT EXISTS DataWarehouse;

-- Switch context to the new database
\c DataWarehouse;

-- Create schema layers for the data warehouse
CREATE SCHEMA IF NOT EXISTS bronze;   -- Raw data storage layer
CREATE SCHEMA IF NOT EXISTS silver;   -- Cleaned and transformed layer
CREATE SCHEMA IF NOT EXISTS gold;     -- Curated/analytics-ready layer
