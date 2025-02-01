--------------------------------------------------------------------------
-- prepare monitor & warehouse (loader, transformer, reader)
--------------------------------------------------------------------------
USE ROLE accountadmin;
-- set daily quota for account
CREATE OR REPLACE RESOURCE MONITOR account_monitor
  WITH credit_quota = 10 frequency = daily start_timestamp = immediately
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND ON 110 PERCENT DO SUSPEND_IMMEDIATE;
ALTER ACCOUNT SET RESOURCE_MONITOR = account_monitor;

-- set daily quota for warehouse
CREATE OR REPLACE RESOURCE MONITOR bikeshare_monitor_wh
  WITH credit_quota = 10 frequency = daily start_timestamp = immediately
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND ON 110 PERCENT DO SUSPEND_IMMEDIATE;

-- create warehouse
create warehouse bikeshare_loading_wh -- 👨‍🏭
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    resource_monitor = bikeshare_monitor_wh;
create warehouse bikeshare_transforming_wh -- 👨‍🔧
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    resource_monitor = bikeshare_monitor_wh;
create warehouse bikeshare_reading_wh -- 🕵️‍♂️
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    resource_monitor = bikeshare_monitor_wh;

show warehouses;

--------------------------------------------------------------------------
-- prepare db & schema & warehouse
--------------------------------------------------------------------------
USE ROLE sysadmin;
USE WAREHOUSE compute_wh;

create database bikeshare;
create or replace schema bronze comment = "🚲🥉 stores raw data";
create or replace schema silver comment = "🚲🥈 stores staging & intermediate data";
create or replace schema gold comment = "🚲🥇 stores data ready for use by analysts & viz/bi tools";

show schemas in database bikeshare;

--------------------------------------------------------------------------
-- prepare roles (admin, loader, transformer, reader)
--------------------------------------------------------------------------
USE ROLE securityadmin;

-- create main roles
CREATE or replace ROLE bikeshare_admin comment = "🚲 admin role for bikeshare domain";
CREATE or replace ROLE bikeshare_loader comment = "🚲 Loads data in 🥉 bronze layer (raw data)";
CREATE or replace ROLE bikeshare_transformer comment = "🚲 Transforms data into silver & gold layers (🥈 staging/intermediate 🥇 datamart with dim & fct) (ex: dbt)";
CREATE or replace ROLE bikeshare_reader comment = "🚲 Reads data from all layers 🥇🥈🥉 (ex: power bi, analyste)";

-- set role depedencies & hook it to sysadmin
grant role bikeshare_admin TO ROLE sysadmin;
grant role bikeshare_loader TO ROLE bikeshare_admin;
grant role bikeshare_transformer TO ROLE bikeshare_admin;
grant role bikeshare_reader TO ROLE bikeshare_transformer;

-------------------------------
-- grants on db objects

-- set ownership to main schemas
GRANT ownership ON schema bikeshare.bronze TO ROLE bikeshare_loader;
GRANT ownership ON schema bikeshare.silver TO ROLE bikeshare_transformer;
GRANT ownership ON schema bikeshare.gold TO ROLE bikeshare_transformer;

-- grant read to reader on all schemas
GRANT USAGE ON DATABASE bikeshare TO role bikeshare_reader;
GRANT USAGE ON ALL SCHEMAS IN DATABASE bikeshare TO ROLE bikeshare_reader;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE bikeshare TO ROLE bikeshare_reader;
GRANT SELECT ON ALL TABLES IN DATABASE bikeshare TO ROLE bikeshare_reader;
GRANT SELECT ON FUTURE TABLES IN DATABASE bikeshare TO ROLE bikeshare_reader;
GRANT SELECT ON ALL VIEWS IN DATABASE bikeshare TO ROLE bikeshare_reader;
GRANT SELECT ON FUTURE VIEWS IN DATABASE bikeshare TO ROLE bikeshare_reader;

show roles;
show grants on role bikeshare_reader;

-------------------------------
-- grants on warehouse

grant all on warehouse bikeshare_loading_wh to role bikeshare_loader;
grant all on warehouse bikeshare_transforming_wh to role bikeshare_transformer;
grant all on warehouse bikeshare_reading_wh to role bikeshare_reader;

--------------------------------------------------------------------------
-- prepare service account
--------------------------------------------------------------------------
/*
```bash
# setup ssh key for your service account
ssh-keygen -t rsa -b 2048 -m pkcs8 -C "agiraud_snow" -f key_agiraud_snowflake
# show the public key to setup in snowflake (special format required)
ssh-keygen -e -f .\key_agiraud_snowflake.pub -m pkcs8
# copy past it in RSA_PUBLIC_KEY
```
*/

USE ROLE USERADMIN;
-- loader
CREATE OR REPLACE USER loader_pc_ag_rog
    type = SERVICE
    DEFAULT_ROLE = bikeshare_loader
    DEFAULT_WAREHOUSE = bikeshare_loading_wh
    DEFAULT_NAMESPACE = bikeshare.bronze
    COMMENT = "PC d'antoine : asus rog"
    RSA_PUBLIC_KEY = 'MIIBxxxxxx';
-- transformer
CREATE OR REPLACE USER transformer_pc_ag_rog
    type = SERVICE
    DEFAULT_ROLE = bikeshare_transformer
    DEFAULT_WAREHOUSE = bikeshare_transforming_wh
    DEFAULT_NAMESPACE = bikeshare.silver
    COMMENT = "PC d'antoine : asus rog"
    RSA_PUBLIC_KEY = 'MIIBxxxxxx';
-- reader
CREATE OR REPLACE USER reader_pc_ag_rog
    type = SERVICE
    DEFAULT_ROLE = bikeshare_reader
    DEFAULT_WAREHOUSE = bikeshare_reading_wh
    DEFAULT_NAMESPACE = bikeshare.gold
    COMMENT = "PC d'antoine : asus rog"
    RSA_PUBLIC_KEY = 'MIIBxxxxxx';
-- ALTER USER loader_pc_ag_rog SET RSA_PUBLIC_KEY_2='3QIDAQAB';

show users;


--------------------------------------------------------------------------
-- grants for role bikeshare_loader
--------------------------------------------------------------------------
USE ROLE securityadmin;

GRANT ROLE bikeshare_loader TO USER loader_pc_ag_rog;
GRANT ROLE bikeshare_transformer TO USER transformer_pc_ag_rog;
GRANT ROLE bikeshare_reader TO USER reader_pc_ag_rog;
GRANT ROLE bikeshare_admin TO USER agiraudemo;

show grants on role bikeshare_loader;
show grants on user reader_pc_ag_rog;
