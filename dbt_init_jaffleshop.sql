use role sysadmin;
use warehouse compute_wh;
use database demo_dbt_jaffleshop;

drop database demo_dbt_jaffleshop;
create database demo_dbt_jaffleshop;


CREATE OR REPLACE FILE FORMAT demo_dbt_jaffleshop.public.csv_ff_jaffle
type = 'csv' field_delimiter = ',' skip_header = 1;

--------------------------------------------------------------------------
-- prepare jaffle_shop raw data
--------------------------------------------------------------------------
drop schema raw_jaffle_shop;
create schema if not exists raw_jaffle_shop;

create table raw_jaffle_shop.raw_customers (
    id integer,
    first_name varchar,
    last_name varchar
);

copy into raw_jaffle_shop.raw_customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = demo_dbt_jaffleshop.public.csv_ff_jaffle;

create table raw_jaffle_shop.raw_orders (
    id integer,
    user_id integer,
    order_date date,
    status varchar,
    _etl_loaded_at timestamp default current_timestamp
);

copy into raw_jaffle_shop.raw_orders (id, user_id, order_date, status)
from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format = demo_dbt_jaffleshop.public.csv_ff_jaffle;


--------------------------------------------------------------------------
-- prepare jaffle_shop raw data
--------------------------------------------------------------------------
create schema if not exists raw_stripe;

create table raw_stripe.raw_payment (
    id integer,
    orderid integer,
    paymentmethod varchar,
    status varchar,
    amount integer,
    created date,
    _batched_at timestamp default current_timestamp
);

copy into raw_stripe.raw_payment (id, orderid, paymentmethod, status, amount, created)
from 's3://dbt-tutorial-public/stripe_payments.csv'
file_format = demo_dbt_jaffleshop.public.csv_ff_jaffle;

select * from raw_jaffle_shop.raw_customers;
select * from raw_jaffle_shop.raw_orders;
select * from raw_stripe.raw_payment;


drop table RAW_STRIPE.PAYMENT;

--------------------------------------------------------------------------
-- next up with role dbt_runner
--------------------------------------------------------------------------
use role dbt_runner;
