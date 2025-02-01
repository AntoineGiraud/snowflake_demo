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
  FILE_FORMAT = ( FORMAT_NAME= 'parquet_ff' ) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
-- 19s

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