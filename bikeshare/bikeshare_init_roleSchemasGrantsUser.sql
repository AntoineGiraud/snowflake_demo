/*
roles :
- bikeshare_admin (granted to sysadmin)
  - bikeshare_loader       using 💻 `bikeshare_loading_wh`      owns 🥉 bonze schemas
  - bikeshare_transformer  using 💻 `bikeshare_transforming_wh` owns 🥈 silver & 🥇 gold schemas
    - bikeshare_reader     using 💻 `bikeshare_reading_wh`      reads all schemas 🥉🥈🥇
*/
--------------------------------------------------------------------------
-- prepare monitor & warehouses (loader, transformer, reader)
--------------------------------------------------------------------------
use role accountadmin;

-- set daily quota for account
create or replace resource monitor account_monitor
  with credit_quota = 10 frequency = daily start_timestamp = immediately
  triggers on 80 percent do notify on 100 percent do suspend on 110 percent do suspend_immediate;
alter account set resource_monitor = account_monitor;

-- set daily quota for warehouse
create or replace resource monitor bikeshare_monitor_wh
  with credit_quota = 10 frequency = daily start_timestamp = immediately
  triggers on 80 percent do notify on 100 percent do suspend on 110 percent do suspend_immediate;

-- create warehouse
create or replace warehouse bikeshare_loading_wh -- 👨‍🏭
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    resource_monitor = bikeshare_monitor_wh;
create or replace warehouse bikeshare_transforming_wh -- 👨‍🔧
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    resource_monitor = bikeshare_monitor_wh;
create or replace warehouse bikeshare_reading_wh -- 🕵️‍♂️
    warehouse_size = xsmall auto_suspend = 60 auto_resume = true initially_suspended = true
    resource_monitor = bikeshare_monitor_wh;

show warehouses;

-- par défaut sys admin n'est plus owner de warehouse compute_wh
use role securityadmin;
grant all on warehouse compute_wh to role sysadmin;

--------------------------------------------------------------------------
-- prepare db & schema
--------------------------------------------------------------------------
use role sysadmin;
use WAREHOuse compute_wh;

create database bikeshare;
create or replace schema bronze comment = "🚲🥉 stores raw data";
create or replace schema silver comment = "🚲🥈 stores staging & intermediate data";
create or replace schema gold comment = "🚲🥇 stores data ready for use by analysts & viz/bi tools";

show schemas in database bikeshare;

--------------------------------------------------------------------------
-- prepare roles (admin, loader, transformer, reader)
--------------------------------------------------------------------------
use role securityadmin;

-- create main roles
create or replace role bikeshare_admin comment = "🚲 admin role for bikeshare domain";
create or replace role bikeshare_loader comment = "🚲 Loads data in 🥉 bronze layer (raw data)";
create or replace role bikeshare_transformer comment = "🚲 Transforms data into silver & gold layers (🥈 staging/intermediate 🥇 datamart with dim & fct) (ex: dbt)";
create or replace role bikeshare_reader comment = "🚲 Reads data from all layers 🥇🥈🥉 (ex: power bi, analyste)";

show roles;

-- set role depedencies & hook it to sysadmin
grant role bikeshare_admin to role sysadmin;
grant role bikeshare_loader to role bikeshare_admin;
grant role bikeshare_transformer to role bikeshare_admin;
grant role bikeshare_reader to role bikeshare_transformer;

-------------------------------
-- grants on db objects

-- set ownership to main schemas
grant ownership ON schema bikeshare.bronze to role bikeshare_loader;
grant ownership ON schema bikeshare.silver to role bikeshare_transformer;
grant ownership ON schema bikeshare.gold to role bikeshare_transformer;

grant USAGE ON DATABASE bikeshare to role bikeshare_loader;
-- grant read to reader on all schemas
grant USAGE ON DATABASE bikeshare to role bikeshare_reader;
grant USAGE ON ALL SCHEMAS IN DATABASE bikeshare to role bikeshare_reader;
grant USAGE ON FUTURE SCHEMAS IN DATABASE bikeshare to role bikeshare_reader;
grant SELECT ON ALL TABLES IN DATABASE bikeshare to role bikeshare_reader;
grant SELECT ON FUTURE TABLES IN DATABASE bikeshare to role bikeshare_reader;
grant SELECT ON ALL VIEWS IN DATABASE bikeshare to role bikeshare_reader;
grant SELECT ON FUTURE VIEWS IN DATABASE bikeshare to role bikeshare_reader;

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
# copy past it in rsa_public_key
```
*/

use role USERADMIN;
-- loader
create or replace user loader_pc_ag_rog
    type = service
    default_role = bikeshare_loader
    default_warehouse = bikeshare_loading_wh
    default_namespace = bikeshare.bronze
    comment = "PC d'antoine : asus rog";
    -- rsa_public_key = 'MIIBxxxxxx';
-- transformer
create or replace user transformer_pc_ag_rog
    type = service
    default_role = bikeshare_transformer
    default_warehouse = bikeshare_transforming_wh
    default_namespace = bikeshare.silver
    comment = "PC d'antoine : asus rog";
    --rsa_public_key = 'MIIBxxxxxx';
-- reader
create or replace user reader_pc_ag_rog
    type = service
    default_role = bikeshare_reader
    default_warehouse = bikeshare_reading_wh
    default_namespace = bikeshare.gold
    comment = "PC d'antoine : asus rog";
    -- rsa_public_key = 'MIIBxxxxxx';
alter user loader_pc_ag_rog set rsa_public_key_2='3QIDAQAB';
alter user transformer_pc_ag_rog set rsa_public_key_2='3QIDAQAB';
alter user reader_pc_ag_rog set rsa_public_key_2='3QIDAQAB';

show users;


--------------------------------------------------------------------------
-- grants for role bikeshare_loader
--------------------------------------------------------------------------
use role securityadmin;

grant role bikeshare_loader to user loader_pc_ag_rog;
grant role bikeshare_transformer to user transformer_pc_ag_rog;
grant role bikeshare_reader to user reader_pc_ag_rog;
grant role bikeshare_admin to user agiraudemo;

show grants on role bikeshare_loader;
show grants on user reader_pc_ag_rog;

--------------------------------------------------------------------------
-- 🧨 drop all
--------------------------------------------------------------------------
use role accountadmin;
-- database
drop database if exists bikeshare cascade;
-- roles
drop role if exists bikeshare_loader;
drop role if exists bikeshare_reader;
drop role if exists bikeshare_transformer;
drop role if exists bikeshare_admin;
-- users
drop user if exists loader_pc_ag_rog;
drop user if exists transformer_pc_ag_rog;
drop user if exists reader_pc_ag_rog;
-- warehouses
drop warehouse if exists bikeshare_loading_wh;
drop warehouse if exists bikeshare_transforming_wh;
drop warehouse if exists bikeshare_reading_wh;
-- ressource monitor
drop resource monitor if exists bikeshare_monitor_wh;
drop resource monitor if exists account_monitor;