----------------------------------------------------------------------------------
-- intro: load data
----------------------------------------------------------------------------------

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

---> create the Tasty Bytes Database
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;

---> create the Raw POS (Point-of-Sale) Schema
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;

---> create the Raw Menu Table
CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

---> create the Stage referencing the Blob location and CSV File Format
CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

---> query the Stage to find the Menu CSV file
LIST @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

---> copy the Menu file into the Menu table
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

----------------------------------------------------------------------------------
-- explore data
----------------------------------------------------------------------------------

-- How many items are there with an item_category of 'Snack' and an item_subcategory of 'Warm Option'?

describe table raw_pos.menu;

select
    item_category,
    item_subcategory,
    count(1) nb,
from raw_pos.menu
group by all
order by 1,2;


select
    item_subcategory,
    count(1) nb,
    max(SALE_PRICE_USD) max_price,
from raw_pos.menu
group by all
order by 1,2;

----------------------------------------------------------------------------------
-- warehouse
----------------------------------------------------------------------------------

CREATE WAREHOUSE warehouse_dash;
SHOW WAREHOUSES;
USE WAREHOUSE COMPUTE_WH;
USE WAREHOUSE warehouse_dash;
---> set warehouse size to medium
ALTER WAREHOUSE warehouse_dash SET warehouse_size=MEDIUM;
---> set warehouse size to xsmall
ALTER WAREHOUSE warehouse_dash SET warehouse_size=XSMALL;
---> set the auto_suspend and auto_resume parameters
ALTER WAREHOUSE warehouse_dash SET AUTO_SUSPEND = 180 AUTO_RESUME = FALSE;
---> drop warehouse
DROP WAREHOUSE warehouse_dash;
SHOW WAREHOUSES;
---> create a multi-cluster warehouse (max clusters = 3)
CREATE WAREHOUSE warehouse_vino MAX_CLUSTER_COUNT = 3;
SHOW WAREHOUSES;
DROP WAREHOUSE warehouse_vino;
SHOW WAREHOUSES;


----------------------------------------------------------------------------------
-- stage
----------------------------------------------------------------------------------

USE ROLE accountadmin;

/*--
database, schema and warehouse creation
--*/

-- create tasty_bytes database
CREATE OR REPLACE DATABASE tasty_bytes;

-- create raw_pos schema
CREATE OR REPLACE SCHEMA tasty_bytes.raw_pos;

-- create raw_customer schema
CREATE OR REPLACE SCHEMA tasty_bytes.raw_customer;

-- create harmonized schema
CREATE OR REPLACE SCHEMA tasty_bytes.harmonized;

-- create analytics schema
CREATE OR REPLACE SCHEMA tasty_bytes.analytics;

-- create warehouse for ingestion
CREATE OR REPLACE WAREHOUSE demo_build_wh
    WAREHOUSE_SIZE = 'xlarge'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

/*--
file format and stage creation
--*/

CREATE OR REPLACE FILE FORMAT tasty_bytes.public.csv_ff
type = 'csv';

CREATE OR REPLACE STAGE tasty_bytes.public.s3load
url = 's3://sfquickstarts/tasty-bytes-builder-education/'
file_format = tasty_bytes.public.csv_ff;

/*--
 raw zone table build
--*/

-- country table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
);

-- franchise table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.franchise
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

-- location table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
);

-- menu table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

-- truck table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- order_header table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

-- order_detail table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.order_detail
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
);

-- customer loyalty table build
CREATE OR REPLACE TABLE tasty_bytes.raw_customer.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

/*--
harmonized view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW tasty_bytes.harmonized.orders_v
    AS
SELECT
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM tasty_bytes.raw_pos.order_detail od
JOIN tasty_bytes.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN tasty_bytes.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN tasty_bytes.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN tasty_bytes.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN tasty_bytes.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

-- loyalty_metrics_v view
CREATE OR REPLACE VIEW tasty_bytes.harmonized.customer_loyalty_metrics_v
    AS
SELECT
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tasty_bytes.raw_customer.customer_loyalty cl
JOIN tasty_bytes.raw_pos.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;

/*--
analytics view creation
--*/

-- orders_v view
CREATE OR REPLACE VIEW tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT DATE(o.order_ts) AS date, * FROM tasty_bytes.harmonized.orders_v o;

-- customer_loyalty_metrics_v view
CREATE OR REPLACE VIEW tasty_bytes.analytics.customer_loyalty_metrics_v
COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
    AS
SELECT * FROM tasty_bytes.harmonized.customer_loyalty_metrics_v;

/*--
 raw zone table load
--*/

USE WAREHOUSE demo_build_wh;

-- country table load
COPY INTO tasty_bytes.raw_pos.country
FROM @tasty_bytes.public.s3load/raw_pos/country/;

-- franchise table load
COPY INTO tasty_bytes.raw_pos.franchise
FROM @tasty_bytes.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO tasty_bytes.raw_pos.location
FROM @tasty_bytes.public.s3load/raw_pos/location/;

-- menu table load
COPY INTO tasty_bytes.raw_pos.menu
FROM @tasty_bytes.public.s3load/raw_pos/menu/;

-- truck table load
COPY INTO tasty_bytes.raw_pos.truck
FROM @tasty_bytes.public.s3load/raw_pos/truck/;

-- customer_loyalty table load
COPY INTO tasty_bytes.raw_customer.customer_loyalty
FROM @tasty_bytes.public.s3load/raw_customer/customer_loyalty/;

-- order_header table load
COPY INTO tasty_bytes.raw_pos.order_header
FROM @tasty_bytes.public.s3load/raw_pos/subset_order_header/;

-- order_detail table load
COPY INTO tasty_bytes.raw_pos.order_detail
FROM @tasty_bytes.public.s3load/raw_pos/subset_order_detail/;

DROP WAREHOUSE demo_build_wh;

----------------------------------------------------------------------------------
-- stage 2
----------------------------------------------------------------------------------
USE WAREHOUSE compute_wh;
CREATE DATABASE test_ingestion;
CREATE OR REPLACE FILE FORMAT test_ingestion.public.csv_ff
type = 'csv';

CREATE OR REPLACE STAGE test_ingestion.public.test_stage
url = 's3://sfquickstarts/tasty-bytes-builder-education/raw_pos/truck/'
file_format = test_ingestion.public.csv_ff;

ls @test_ingestion.public.test_stage/;

-- truck table build
CREATE OR REPLACE TABLE test_ingestion.public.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

-- order_detail table load
COPY INTO test_ingestion.public.truck
FROM @test_ingestion.public.test_stage;


----------------------------------------------------------------------------------
-- Database & schemas
----------------------------------------------------------------------------------

SHOW DATABASES; -- all accross
SHOW SCHEMAS; -- all accross
SHOW TABLES; -- all accross
---> drop the database
DROP DATABASE test_ingestion;
---> see metadata about your database
DESCRIBE DATABASE test_ingestion;
DESCRIBE SCHEMA test_ingestion.public;

create DATABASE TEST_DATABASE;
Drop DATABASE TEST_DATABASE;
use DATABASE TEST_DATABASE;
create schema TEST_SCHEMA;
SHOW SCHEMAS; -- all accross
DESCRIBE DATABASE TEST_DATABASE;
unDrop schema TEST_SCHEMA;

----------------------------------------------------------------------------------
-- tables
----------------------------------------------------------------------------------

---> create a table – note that each column has a name and a data type
CREATE TABLE TEST_TABLE (
	TEST_NUMBER NUMBER,
	TEST_VARCHAR VARCHAR,
	TEST_BOOLEAN BOOLEAN,
	TEST_DATE DATE,
	TEST_VARIANT VARIANT,
	TEST_GEOGRAPHY GEOGRAPHY
);

SELECT * FROM TEST_DATABASE.TEST_SCHEMA.TEST_TABLE;

---> insert a row into the table we just created
INSERT INTO TEST_DATABASE.TEST_SCHEMA.TEST_TABLE
  VALUES
  (28, 'ha!', True, '2024-01-01', NULL, NULL);

SELECT * FROM TEST_DATABASE.TEST_SCHEMA.TEST_TABLE;

---> drop the test table
DROP TABLE TEST_DATABASE.TEST_SCHEMA.TEST_TABLE;

---> see all tables in a particular schema
SHOW TABLES IN TEST_DATABASE.TEST_SCHEMA;

---> undrop the test table
UNDROP TABLE TEST_DATABASE.TEST_SCHEMA.TEST_TABLE;

SHOW TABLES IN TEST_DATABASE.TEST_SCHEMA;

SHOW TABLES;

---> see table storage metadata from the Snowflake database
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

SHOW TABLES;

-----------------------------------------
-- exo
CREATE TABLE test_table2 (
	TEST_NUMBER NUMBER
);
INSERT INTO TEST_DATABASE.TEST_SCHEMA.test_table2
  VALUES
  (42);
----------------------------------------------------------------------------------
-- views
----------------------------------------------------------------------------------

---> create the orders_v view – note the “CREATE VIEW view_name AS SELECT” syntax
CREATE VIEW tasty_bytes.harmonized.orders_v
    AS
SELECT
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM tasty_bytes.raw_pos.order_detail od
JOIN tasty_bytes.raw_pos.order_header oh
    ON od.order_id = oh.order_id
JOIN tasty_bytes.raw_pos.truck t
    ON oh.truck_id = t.truck_id
JOIN tasty_bytes.raw_pos.menu m
    ON od.menu_item_id = m.menu_item_id
JOIN tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id
JOIN tasty_bytes.raw_pos.location l
    ON oh.location_id = l.location_id
LEFT JOIN tasty_bytes.raw_customer.customer_loyalty cl
    ON oh.customer_id = cl.customer_id;

SELECT COUNT(*) FROM tasty_bytes.harmonized.orders_v;

CREATE VIEW tasty_bytes.harmonized.brand_names
    AS
SELECT truck_brand_name
FROM tasty_bytes.raw_pos.menu;

SHOW VIEWS;

---> drop a view
DROP VIEW tasty_bytes.harmonized.brand_names;

SHOW VIEWS;

---> see metadata about a view
DESCRIBE VIEW tasty_bytes.harmonized.orders_v;

---> create a materialized view
CREATE MATERIALIZED VIEW tasty_bytes.harmonized.brand_names_materialized
    AS
SELECT DISTINCT truck_brand_name
FROM tasty_bytes.raw_pos.menu;

SELECT * FROM tasty_bytes.harmonized.brand_names_materialized;

SHOW VIEWS;

SHOW MATERIALIZED VIEWS;

---> see metadata about the materialized view we just made
DESCRIBE VIEW tasty_bytes.harmonized.brand_names_materialized;

DESCRIBE MATERIALIZED VIEW tasty_bytes.harmonized.brand_names_materialized;

---> drop the materialized view
DROP MATERIALIZED VIEW tasty_bytes.harmonized.brand_names_materialized;

----------------------------------
-- exo

create or replace view tasty_bytes.harmonized.truck_franchise
-- create or replace materialized view tasty_bytes.harmonized.truck_franchise_materialized -- erreur car join
as
SELECT
    t.*,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name
FROM tasty_bytes.raw_pos.truck t
JOIN tasty_bytes.raw_pos.franchise f
    ON t.franchise_id = f.franchise_id;

select *
from truck_franchise
where lower(franchisee_first_name)='sara'
    and lower(franchisee_last_name)='nicholson';

DESCRIBE VIEW truck_franchise;
drop VIEW truck_franchise;

create MATERIALIZED VIEW nissan as
SELECT
    t.*
FROM tasty_bytes.raw_pos.truck t
WHERE make = 'Nissan';

select count(1) nb from nissan;

drop MATERIALIZED VIEW nissan;

----------------------------------------------------------------------------------
-- semi structured stuff
----------------------------------------------------------------------------------

---> see an example of a column with semi-structured (JSON) data
SELECT MENU_ITEM_NAME
    , MENU_ITEM_HEALTH_METRICS_OBJ
FROM tasty_bytes.RAW_POS.MENU;

DESCRIBE TABLE tasty_bytes.RAW_POS.MENU;
select TYPEOF(MENU_ITEM_HEALTH_METRICS_OBJ)
from tasty_bytes.RAW_POS.MENU;

select MENU_ITEM_HEALTH_METRICS_OBJ['menu_item_health_metrics'][0]['ingredients'][0]
FROM tasty_bytes.raw_pos.menu
WHERE MENU_ITEM_NAME = 'Mango Sticky Rice';


---> create the test_menu table with just a variant column in it, as a test
CREATE TABLE tasty_bytes.RAW_POS.TEST_MENU (cost_of_goods_variant)
AS SELECT cost_of_goods_usd::VARIANT
FROM tasty_bytes.RAW_POS.MENU;

---> notice that the column is of the VARIANT type
DESCRIBE TABLE tasty_bytes.RAW_POS.TEST_MENU;

---> but the typeof() function reveals the underlying data type
SELECT TYPEOF(cost_of_goods_variant) FROM tasty_bytes.raw_pos.test_menu;

---> Snowflake lets you perform operations based on the underlying data type
SELECT cost_of_goods_variant, cost_of_goods_variant*2.0 FROM tasty_bytes.raw_pos.test_menu;

DROP TABLE tasty_bytes.raw_pos.test_menu;

---> you can use the colon to pull out info from menu_item_health_metrics_obj
SELECT MENU_ITEM_HEALTH_METRICS_OBJ:menu_item_health_metrics FROM tasty_bytes.raw_pos.menu;

---> use typeof() to see the underlying type
SELECT TYPEOF(MENU_ITEM_HEALTH_METRICS_OBJ) FROM tasty_bytes.raw_pos.menu;

SELECT MENU_ITEM_HEALTH_METRICS_OBJ, MENU_ITEM_HEALTH_METRICS_OBJ['menu_item_id'] FROM tasty_bytes.raw_pos.menu;



SELECT DISTINCT primary_city
FROM tasty_bytes.analytics.orders_v
ORDER BY primary_city
