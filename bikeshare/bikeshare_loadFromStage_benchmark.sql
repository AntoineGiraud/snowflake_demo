use role bikeshare_loader;
use warehouse bikeshare_loading_wh;
use schema bikeshare.bronze;

--------------------------------------------------
-- create file formats
--------------------------------------------------
CREATE OR REPLACE FILE FORMAT parquet_ff_slow
    type = 'parquet'
    USE_LOGICAL_TYPE = TRUE
    USE_VECTORIZED_SCANNER = false;
    -- interpret Parquet logical types during data loading
    -- why: https://community.snowflake.com/s/article/How-to-load-logical-type-TIMESTAMP-data-from-Parquet-files-into-Snowflake

CREATE OR REPLACE FILE FORMAT parquet_ff
    type = 'parquet'
    USE_LOGICAL_TYPE = TRUE
    USE_VECTORIZED_SCANNER = true;
    -- faster ... will be default value soon
    -- https://www.snowflake.com/en/engineering-blog/loading-terabytes-into-snowflake-speeds-feeds-techniques/


CREATE OR REPLACE FILE FORMAT csv_ff
    type = 'csv' SKIP_HEADER=1;
    -- faster ... will be default value soon
    -- https://www.snowflake.com/en/engineering-blog/loading-terabytes-into-snowflake-speeds-feeds-techniques/


--------------------------------------------------
-- create internal stage ... to load files into
--------------------------------------------------
-- ménage parquet actuels
drop STAGE if exists stage_parquet;
-- add stage to drop parquet in
CREATE or replace STAGE stage_parquet
  FILE_FORMAT = (FORMAT_NAME = 'parquet_ff');

-- from SnowSQL
-- PUT file://myFolder/*.parquet @stage_parquet;

--------------------------------------------------
-- exporing stage & read parquet with select
--------------------------------------------------
list @stage_parquet;

-- read parquet as is
select count(*) nb
FROM  @stage_parquet/rentals_2021  (FILE_FORMAT => 'parquet_ff');

--------------------------------------------------
-- loading .parquet into snowflake table
--------------------------------------------------

-- create table based on schema detection
CREATE or replace TABLE rentals_2020_parquet USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) -- discover schema from stage files :)
      -- select *
      FROM TABLE( INFER_SCHEMA( LOCATION=>'@stage_parquet/rentals_2020.parquet', FILE_FORMAT=>'parquet_ff' )) -- where TYPE in ('BINARY')
    );

-- Load the parquet file using MATCH_BY_COLUMN_NAME.
COPY INTO rentals_2020_parquet FROM @stage_parquet/rentals_2020.parquet
  FILE_FORMAT = ( type=parquet use_vectorized_scanner=true USE_LOGICAL_TYPE = TRUE)
  MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
  ON_ERROR = ABORT_STATEMENT;
-- 19s -> case_INsensitive
-- 13s -> case_sensitive

-- R&D https://www.snowflake.com/en/blog/faster-batch-ingestion-for-parquet
----> INSERT INTO insted of COPY INTO ... 💁🕵️

-- tada
select * from rentals limit 20;

--------------------------------------------------
-- loading .csv into snowflake table
--------------------------------------------------

CREATE or replace TABLE rentals_2020_csv_gz as select * from rentals_2020_parquet limit 0;
CREATE or replace TABLE rentals_2020_csv_raw as select * from rentals_2020_parquet limit 0;

--9s
COPY INTO rentals_2020_csv_gz FROM @stage_parquet/rentals_2020_gz.csv.gz  FILE_FORMAT = ( FORMAT_NAME= 'csv_ff' ) ;
--9s
COPY INTO rentals_2020_csv_raw FROM @stage_parquet/rentals_2020.csv  FILE_FORMAT = ( FORMAT_NAME= 'csv_ff' ) ;


select 'rentals_2020_parquet' src, count(1) nb from rentals_2020
union all
select 'rentals_2020_csv_gz' src, count(1) nb from rentals_2020_csv_gz
union all
select 'rentals_2020_csv_raw' src, count(1) nb from rentals_2020_csv_raw
;

REMOVE  @stage_parquet/rentals_2020.csv.gz;

-- 0.644s
select "start_date_month", count(1) nb_rentals,
    count(distinct "start_date") nb_date
from rentals_2020_parquet
group by 1;


select * from rentals_2020_parquet limit 10;

--------------------------------------------------
-- gcs external storage
--------------------------------------------------
-- doc: https://docs.snowflake.com/en/user-guide/data-load-gcs-config
use role accountadmin;

SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO();

CREATE STORAGE INTEGRATION gcs_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://bikeshare_ag/');

DESC STORAGE INTEGRATION gcs_int;

-- grant usage on gcs
use role securityadmin;
GRANT USAGE ON INTEGRATION gcs_int TO ROLE bikeshare_loader;

-- create external gcs stage
use role bikeshare_loader;
CREATE STAGE gcs_stage_bikeshare
  URL = 'gcs://bikeshare_ag/'
  STORAGE_INTEGRATION = gcs_int
  FILE_FORMAT = parquet_ff;

-- read .parquet metadata
select *
FROM TABLE( INFER_SCHEMA( LOCATION=>'@gcs_stage_bikeshare/rentals', FILE_FORMAT=>'parquet_ff' ));

-- external table
CREATE or replace EXTERNAL TABLE ext_gcs_rentals
(
    filename string as (metadata$filename::string),
    start_date_month DATE as (value:start_date_month::DATE),
    start_date DATE as (value:start_date::DATE),
    start_hour NUMBER(38, 0) as (value:start_hour::NUMBER(38, 0)),
    start_time_15min NUMBER(38, 0) as (value:start_time_15min::NUMBER(38, 0)),
    duration_5min_group REAL as (value:duration_5min_group::REAL),
    duration_min REAL as (value:duration_min::REAL),
    start_station_year_code TEXT as (value:start_station_year_code::TEXT),
    end_station_year_code TEXT as (value:end_station_year_code::TEXT),
    is_member NUMBER(38, 0) as (value:is_member::NUMBER(38, 0))
)
  PARTITION BY (filename)
  LOCATION=@gcs_stage_bikeshare/rentals/
  AUTO_REFRESH = false
  FILE_FORMAT = 'parquet_ff';

select * from ext_gcs_rentals limit 10;
select * from ext_gcs_rentals_snow limit 10;

-- 18s
create or replace table ext_gcs_rentals_snow as
select * exclude(value)
from ext_gcs_rentals
where filename='rentals/rentals_2020.parquet'
;

truncate table ext_gcs_rentals;

insert into ext_gcs_rentals


----------------------------------------------------------------------

-- create table based on schema detection
CREATE or replace TABLE rentals_2020_parquet_gcs USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) -- discover schema from stage files :)
      -- select *
      FROM TABLE( INFER_SCHEMA( LOCATION=>'@gcs_stage_bikeshare/rentals/rentals_2020.parquet', FILE_FORMAT=>'parquet_ff' )) -- where TYPE in ('BINARY')
    );

-- Load the parquet file using MATCH_BY_COLUMN_NAME.
-- 10s
COPY INTO rentals_2020_parquet_gcs FROM @gcs_stage_bikeshare/rentals/rentals_2020.parquet
  FILE_FORMAT = ( type=parquet use_vectorized_scanner=true USE_LOGICAL_TYPE = TRUE)
  MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
  ON_ERROR = ABORT_STATEMENT;

truncate table rentals_2020_parquet_gcs;

-- 10s
insert into rentals_2020_parquet_gcs
select
    $1:start_date_month::DATE as start_date_month,
    $1:start_date::DATE as start_date,
    $1:start_hour::NUMBER(38, 0) as start_hour,
    $1:start_time_15min::TIME as start_time_15min,
    $1:duration_5min_group::REAL as duration_5min_group,
    $1:duration_min::REAL as duration_min,
    $1:start_station_year_code::TEXT as start_station_year_code,
    $1:end_station_year_code::TEXT as end_station_year_code,
    $1:is_member::NUMBER(38, 0) as is_member,
from '@gcs_stage_bikeshare/rentals/rentals_2020.parquet' ( FILE_FORMAT => 'parquet_ff')
--limit 10
;

-----------------------------------------------
-- essai sans schéma
-----------------------------------------------
CREATE OR REPLACE EXTERNAL TABLE ext_table
 WITH LOCATION = @gcs_stage_bikeshare/rentals/
 auto_refresh=false
 FILE_FORMAT = parquet_ff
 ;

 -- create or replace table ext_table_rentals as
 -- truncate table ext_table_rentals;
 -- insert into ext_table_rentals
 select
    metadata$filename::string as filename,
    value:start_date_month::DATE as start_date_month,
    value:start_date::DATE as start_date,
    value:start_hour::NUMBER(38, 0) as start_hour,
    value:start_time_15min::NUMBER(38, 0) as start_time_15min,
    value:duration_5min_group::REAL as duration_5min_group,
    value:duration_min::REAL as duration_min,
    value:start_station_year_code::TEXT as start_station_year_code,
    value:end_station_year_code::TEXT as end_station_year_code,
    value:is_member::NUMBER(38, 0) as is_member,
 from ext_table
 where FILENAME='rentals/rentals_2020.parquet'
 limit 10
;

select * from ext_table_rentals limit 10;