/*
script purpose :
    creates a new database named "DataWarehouse", additionally, the scripts sets up 3 schemas within the database : 'bronze', 'silver' and 'gold'
*/


CREATE DATABASE DataWarehouse
USE DataWarehouse

CREATE SCHEMA bronze
CREATE SCHEMA silver
CREATE SCHEMA gold
