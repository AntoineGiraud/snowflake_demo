----------------------------------------------------------------------------------
-- time travel
----------------------------------------------------------------------------------

SHOW TABLES;

---> set the data retention time to 90 days
ALTER TABLE TASTY_BYTES.RAW_POS.TEST_MENU SET DATA_RETENTION_TIME_IN_DAYS = 90;

SHOW TABLES;

---> set the data retention time to 1 day
ALTER TABLE TASTY_BYTES.RAW_POS.TEST_MENU SET DATA_RETENTION_TIME_IN_DAYS = 1;

---> clone the truck table
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.truck_dev
    CLONE tasty_bytes.raw_pos.truck;

SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM tasty_bytes.raw_pos.truck_dev t;

---> see how the age should have been calculated
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model,
    (YEAR(CURRENT_DATE()) - t.year) AS truck_age
FROM tasty_bytes.raw_pos.truck_dev t;

---> record the most recent query_id, back when the data was still correct
SET good_data_query_id = LAST_QUERY_ID();

---> view the variable’s value
SELECT $good_data_query_id;

---> record the time, back when the data was still correct
SET good_data_timestamp = CURRENT_TIMESTAMP;

---> view the variable’s value
SELECT $good_data_timestamp;

---> confirm that that worked
SHOW VARIABLES;

---> make the first mistake: calculating the truck’s age incorrectly
SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model,
    (YEAR(CURRENT_DATE()) / t.year) AS truck_age
FROM tasty_bytes.raw_pos.truck_dev t;

---> make the second mistake: calculate age wrong, and overwrite the year!
UPDATE tasty_bytes.raw_pos.truck_dev t
    SET t.year = (YEAR(CURRENT_DATE()) / t.year);

SELECT
    t.truck_id,
    t.year,
    t.make,
    t.model
FROM tasty_bytes.raw_pos.truck_dev t;

---> select the data as of a particular timestamp
SELECT * FROM tasty_bytes.raw_pos.truck_dev
AT(TIMESTAMP => $good_data_timestamp);

SELECT $good_data_timestamp;

---> example code, without a timestamp inserted:

-- SELECT * FROM tasty_bytes.raw_pos.truck_dev
-- AT(TIMESTAMP => '[insert timestamp]'::TIMESTAMP_LTZ);

--->example code, with a timestamp inserted
SELECT * FROM tasty_bytes.raw_pos.truck_dev
AT(TIMESTAMP => '2024-04-04 21:34:31.833 -0700'::TIMESTAMP_LTZ);

---> calculate the right offset
SELECT TIMESTAMPDIFF(second,CURRENT_TIMESTAMP,$good_data_timestamp);

---> Example code, without an offset inserted:

-- SELECT * FROM tasty_bytes.raw_pos.truck_dev
-- AT(OFFSET => -[WRITE OFFSET SECONDS PLUS A BIT]);

---> select the data as of a particular number of seconds back in time
SELECT * FROM tasty_bytes.raw_pos.truck_dev
AT(OFFSET => -45);

SELECT $good_data_query_id;

---> select the data as of its state before a previous query was run
SELECT * FROM tasty_bytes.raw_pos.truck_dev
BEFORE(STATEMENT => $good_data_query_id);

------------------------------------------------
-- exo - timetravel

CREATE or replace TABLE tasty_bytes.raw_pos.truck_dev
    CLONE tasty_bytes.raw_pos.truck;
SELECT * FROM tasty_bytes.raw_pos.truck_dev;
SET saved_query_id = LAST_QUERY_ID();
SET saved_timestamp = CURRENT_TIMESTAMP;
UPDATE tasty_bytes.raw_pos.truck_dev t
    SET t.year = (YEAR(CURRENT_DATE()) -1000);

show variables;

SELECT *
FROM tasty_bytes.raw_pos.truck_dev AT(TIMESTAMP => $saved_timestamp)
where truck_id=1;

SELECT *
FROM tasty_bytes.raw_pos.truck_dev BEFORE(STATEMENT => $saved_query_id)
where truck_id=2;

-------------------------------------------------
-- transiant & tempory tables
-------------------------------------------------
---> drop truck_dev if not dropped previously
DROP TABLE TASTY_BYTES.RAW_POS.TRUCK_DEV;

---> create a transient table
CREATE TRANSIENT TABLE TASTY_BYTES.RAW_POS.TRUCK_TRANSIENT
    CLONE TASTY_BYTES.RAW_POS.TRUCK;

---> create a temporary table
CREATE TEMPORARY TABLE TASTY_BYTES.RAW_POS.TRUCK_TEMPORARY
    CLONE TASTY_BYTES.RAW_POS.TRUCK;

---> show tables that start with the word TRUCK
SHOW TABLES LIKE 'TRUCK%';

---> attempt (successfully) to set the data retention time to 90 days for the standard table
ALTER TABLE TASTY_BYTES.RAW_POS.TRUCK SET DATA_RETENTION_TIME_IN_DAYS = 90;

---> attempt (unsuccessfully) to set the data retention time to 90 days for the transient table
ALTER TABLE TASTY_BYTES.RAW_POS.TRUCK_TRANSIENT SET DATA_RETENTION_TIME_IN_DAYS = 90;

---> attempt (unsuccessfully) to set the data retention time to 90 days for the temporary table
ALTER TABLE TASTY_BYTES.RAW_POS.TRUCK_TEMPORARY SET DATA_RETENTION_TIME_IN_DAYS = 90;

SHOW TABLES LIKE 'TRUCK%';

---> attempt (successfully) to set the data retention time to 0 days for the transient table
ALTER TABLE TASTY_BYTES.RAW_POS.TRUCK_TRANSIENT SET DATA_RETENTION_TIME_IN_DAYS = 0;

---> attempt (successfully) to set the data retention time to 0 days for the temporary table
ALTER TABLE TASTY_BYTES.RAW_POS.TRUCK_TEMPORARY SET DATA_RETENTION_TIME_IN_DAYS = 0;

SHOW TABLES LIKE 'TRUCK%';

----------------------------------------------------------------------------------
-- cloning object
----------------------------------------------------------------------------------
create database tasty_bytes_clone
  clone tasty_bytes;
create table tasty_bytes_clone.RAW_POS.TRUCK_clone
  clone TASTY_BYTES.RAW_POS.TRUCK;
SELECT * FROM tasty_bytes_clone.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_NAME like 'TRUCK%'
  AND TABLE_CATALOG like 'TASTY_BYTES%';

----------------------------------------------------------------------------------
-- ressource monitor
----------------------------------------------------------------------------------
---> create a resource monitor
CREATE RESOURCE MONITOR tasty_test_rm
WITH
    CREDIT_QUOTA = 10 -- 20 credits
    FREQUENCY = daily -- reset the monitor monthly
    START_TIMESTAMP = immediately -- begin tracking immediately
    TRIGGERS
        ON 80 PERCENT DO NOTIFY -- notify accountadmins at 80%
        ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- suspend warehouse and cancel all queries at 110 percent

---> see all resource monitors
SHOW RESOURCE MONITORS;

---> assign a resource monitor to a warehouse
ALTER WAREHOUSE compute_wh SET RESOURCE_MONITOR = tasty_test_rm;

---> change the credit quota on a resource monitor
ALTER RESOURCE MONITOR tasty_test_rm
  SET CREDIT_QUOTA=12;

---> drop a resource monitor
DROP RESOURCE MONITOR tasty_test_rm;

----------------------------------------------------------------------------------
-- UDF user defined functions
----------------------------------------------------------------------------------

---> here’s an example of a function in action!
SELECT ABS(-14);

---> here’s another example of a function in action!
SELECT UPPER('upper');

---> see all functions
SHOW FUNCTIONS;

SELECT MAX(SALE_PRICE_USD) FROM TASTY_BYTES.RAW_POS.MENU;

---> use a particular database
USE DATABASE TASTY_BYTES;

---> create the max_menu_price function
CREATE FUNCTION max_menu_price()
  RETURNS NUMBER(5,2)
  AS
  $$
    SELECT MAX(SALE_PRICE_USD) FROM TASTY_BYTES.RAW_POS.MENU
  $$
  ;

---> run the max_menu_price function by calling it in a select statement
SELECT max_menu_price();

SHOW FUNCTIONS;

---> create a new function, but one that takes in an argument
CREATE FUNCTION max_menu_price_converted(USD_to_new NUMBER)
  RETURNS NUMBER(5,2)
  AS
  $$
    SELECT USD_TO_NEW*MAX(SALE_PRICE_USD) FROM TASTY_BYTES.RAW_POS.MENU
  $$
  ;

SELECT max_menu_price_converted(1.35);

---> create a Python function
CREATE FUNCTION winsorize (val NUMERIC, up_bound NUMERIC, low_bound NUMERIC)
returns NUMERIC
language python
runtime_version = '3.11'
handler = 'winsorize_py'
AS
$$
def winsorize_py(val, up_bound, low_bound):
    if val > up_bound:
        return up_bound
    elif val < low_bound:
        return low_bound
    else:
        return val
$$;

---> run the Python function
SELECT winsorize(12.0, 11.0, 4.0);

---> here’s the reference UDF we’re going to work off of as we make our UDTF
CREATE FUNCTION min_menu_price()
  RETURNS NUMBER(5,2)
  AS
  $$
    SELECT MIN(SALE_PRICE_USD) FROM TASTY_BYTES.RAW_POS.MENU
  $$
  ;
select min_menu_price();
SHOW FUNCTIONS like '%min_menu_price%';

USE DATABASE TASTY_BYTES;

---> create a user-defined table function
CREATE FUNCTION menu_prices_below(price_ceiling NUMBER)
  RETURNS TABLE (item VARCHAR, price NUMBER)
  AS
  $$
    SELECT MENU_ITEM_NAME, SALE_PRICE_USD
    FROM TASTY_BYTES.RAW_POS.MENU
    WHERE SALE_PRICE_USD < price_ceiling
    ORDER BY 2 DESC
  $$
  ;
SELECT * FROM TABLE(menu_prices_below(3));

---> now you can see it in the list of all functions!
SHOW FUNCTIONS like '%CURRENT_TIMESTAMP%';

---> run the UDTF to see what the output looks like
SELECT * FROM TABLE(menu_prices_above(15));

---> you can use a where clause on the result
SELECT * FROM TABLE(menu_prices_above(15))
WHERE ITEM ILIKE '%CHICKEN%';

----------------------------------------------------------------------------------
-- stored procedure
----------------------------------------------------------------------------------

---> list all procedures
SHOW PROCEDURES;

SELECT * FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER
LIMIT 100;

---> see the latest and earliest order timestamps so we can determine what we want to delete
SELECT MAX(ORDER_TS), MIN(ORDER_TS) FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER;

---> save the max timestamp
SET max_ts = (SELECT MAX(ORDER_TS) FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER);

SELECT $max_ts;

SELECT DATEADD('DAY',-180,$max_ts);

---> determine the necessary cutoff to go back 180 days
SET cutoff_ts = (SELECT DATEADD('DAY',-180,$max_ts));

---> note how you can use the cutoff_ts variable in the WHERE clause
SELECT MAX(ORDER_TS) FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER
WHERE ORDER_TS < $cutoff_ts;

USE DATABASE TASTY_BYTES_clone;

---> create your procedure
CREATE OR REPLACE PROCEDURE delete_old()
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
DECLARE
  max_ts TIMESTAMP;
  cutoff_ts TIMESTAMP;
BEGIN
  max_ts := (SELECT MAX(ORDER_TS) FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER);
  cutoff_ts := (SELECT DATEADD('DAY',-180,:max_ts));
  DELETE FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER
  WHERE ORDER_TS < :cutoff_ts;
END;
$$
;

SHOW PROCEDURES;

---> see information about your procedure
DESCRIBE PROCEDURE delete_old();

---> run your procedure
CALL DELETE_OLD();

---> confirm that that made a difference
SELECT MIN(ORDER_TS) FROM TASTY_BYTES_CLONE.RAW_POS.ORDER_HEADER;

---> it did! We deleted everything from before the cutoff timestamp
SELECT $cutoff_ts;

--------------------------------
-- exo
--------------------------------

CREATE OR REPLACE PROCEDURE increase_prices()
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
BEGIN
  UPDATE tasty_bytes_clone.raw_pos.menu
  SET SALE_PRICE_USD = menu.SALE_PRICE_USD + 1;
END;
$$
;

call increase_prices();
describe procedure increase_prices();
show procedures like 'increase_prices';

----------------------------------------------------------------------------------
-- roles & sécurité
----------------------------------------------------------------------------------
USE ROLE accountadmin;

---> create a role
CREATE ROLE tasty_role;
SHOW GRANTS TO ROLE tasty_role;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE tasty_role;
select current_user;
GRANT ROLE tasty_role TO USER AGIRAUDDEMO;
USE ROLE tasty_role;
CREATE WAREHOUSE tasty_test_wh; -- erreur manque privileges
SHOW GRANTS TO USER AGIRAUDDEMO;
SHOW GRANTS TO role USERADMIN;

GRANT ROLE tasty_de TO USER [username];
---> see what privileges this new role has

---> see what privileges an auto-generated role has
SHOW GRANTS TO ROLE accountadmin;

---> grant a role to a specific user

---> use a role

---> try creating a warehouse with this new role
CREATE WAREHOUSE tasty_de_test;

USE ROLE accountadmin;

---> grant the create warehouse privilege to the tasty_de role

---> show all of the privileges the tasty_de role has
SHOW GRANTS TO ROLE tasty_de;

USE ROLE tasty_de;

---> test to see whether tasty_de can create a warehouse
CREATE WAREHOUSE tasty_de_test;

---> learn more about the privileges each of the following auto-generated roles has
SHOW GRANTS TO ROLE securityadmin;
SHOW GRANTS TO ROLE useradmin;
SHOW GRANTS TO ROLE sysadmin;
SHOW GRANTS TO ROLE public;
