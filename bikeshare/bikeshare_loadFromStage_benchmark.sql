use role bikeshare_loader;
use warehouse bikeshare_loading_wh;
use schema bikeshare.bronze;

--------------------------------------------------
-- create file formats
--------------------------------------------------
CREATE OR REPLACE FILE FORMAT parquet_ff_slow
    type = 'parquet'
    USE_LOGICAL_TYPE = TRUE;
    -- interpret Parquet logical types during data loading
    -- why: https://community.snowflake.com/s/article/How-to-load-logical-type-TIMESTAMP-data-from-Parquet-files-into-Snowflake

CREATE OR REPLACE FILE FORMAT parquet_ff
    type = 'parquet'
    USE_LOGICAL_TYPE = TRUE
    USE_VECTORIZED_SCANNER = true;
    -- faster ... will be default value soon
    -- https://www.snowflake.com/en/engineering-blog/loading-terabytes-into-snowflake-speeds-feeds-techniques/

--------------------------------------------------
-- create internal stage ... to load files into
--------------------------------------------------
-- ménage parquet actuels
drop STAGE if exists stage_parquet;
-- add stage to drop parquet in
CREATE STAGE stage_parquet
  FILE_FORMAT = (FORMAT_NAME = 'parquet_ff');

-- from SnowSQL
-- PUT file://myFolder/*.parquet @stage_parquet;

--------------------------------------------------
-- exporing stage & read parquet with select
--------------------------------------------------
list @stage_parquet;

-- read parquet as is
select count(*) nb
FROM  @stage_parquet/rentals  (FILE_FORMAT => 'parquet_ff');

--------------------------------------------------
-- loading .parquet into snowflake table
--------------------------------------------------

-- create table based on schema detection
CREATE or replace TABLE rentals USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) -- discover schema from stage files :)
      -- select *
      FROM TABLE( INFER_SCHEMA( LOCATION=>'@stage_parquet/rentals', FILE_FORMAT=>'parquet_ff' )) -- where TYPE in ('BINARY')
    );

-- Load the CSV file using MATCH_BY_COLUMN_NAME.
COPY INTO rentals FROM @stage_parquet/rentals
  FILE_FORMAT = ( FORMAT_NAME= 'parquet_ff' ) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;

-- R&D https://www.snowflake.com/en/blog/faster-batch-ingestion-for-parquet
----> INSERT INTO insted of COPY INTO ... 💁🕵️

-- tada
select * from rentals limit 20;
