----------------------------------------------------------------------------------
-- snowpipe - auto ingest from bucket/stage
----------------------------------------------------------------------------------

---> create the storage integration
CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "REMOVED"
  STORAGE_ALLOWED_LOCATIONS = ("s3://intro-to-snowflake-snowpipe/");

---> describe the storage integration to see the info you need to copy over to AWS
DESCRIBE INTEGRATION S3_role_integration;

---> create the database
CREATE OR REPLACE DATABASE S3_db;

---> create the table (automatically in the public schema, because we didn’t specify)
CREATE OR REPLACE TABLE S3_table(food STRING, taste INT);

USE SCHEMA S3_db.public;

---> create stage with the link to the S3 bucket and info on the associated storage integration
CREATE OR REPLACE STAGE S3_stage
  url = ('s3://intro-to-snowflake-snowpipe/')
  storage_integration = S3_role_integration;

SHOW STAGES;

---> see the files in the stage
LIST @S3_stage;

---> select the first two columns from the stage
SELECT $1, $2 FROM @S3_stage;

USE WAREHOUSE COMPUTE_WH;

---> create the snowpipe, copying from S3_stage into S3_table
CREATE PIPE S3_db.public.S3_pipe AUTO_INGEST=TRUE as
  COPY INTO S3_db.public.S3_table
  FROM @S3_db.public.S3_stage;

SELECT * FROM S3_db.public.S3_table;

---> see a list of all the pipes
SHOW PIPES;

DESCRIBE PIPE S3_db.public.S3_pipe;

---> pause the pipe
ALTER PIPE S3_db.public.S3_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

---> drop the pipe
DROP PIPE S3_pipe;

SHOW PIPES;


----------------------------------------------------------------------------------
-- LLM du Cortex Snowflake
----------------------------------------------------------------------------------
---> use the mistral-7b model and Snowflake Cortex Complete to ask a question
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', 'What are three reasons that Snowflake is positioned to become the go-to data platform?');

---> now send the result to the Snowflake Cortex Summarize function
SELECT SNOWFLAKE.CORTEX.SUMMARIZE(SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', 'Pour quel genre de littérature Marianne Moore était-elle connue ?'));

---> run Snowflake Cortex Complete on multiple rows at once
SELECT menu_item_name, SNOWFLAKE.CORTEX.SUMMARIZE(SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b',
        CONCAT('Tell me why this food is tasty: ', menu_item_name)
)) tasty_resume FROM TASTY_BYTES.RAW_POS.MENU LIMIT 5;

---> check out what the table of prompts we’re feeding to Complete (roughly) looks like
SELECT CONCAT('Tell me why this food is tasty: ', menu_item_name)
FROM FROSTBYTE_TASTY_BYTES.RAW_POS.MENU LIMIT 5;

---> give Snowflake Cortex Complete a prompt with history
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b', -- the model you want to use
    [
        {'role': 'system',
        'content': 'Analyze this Snowflake review and determine the overall sentiment. Answer with just \"Positive\", \"Negative\", or \"Neutral\"' },
        {'role': 'user',
        'content': 'I love Snowflake because it is so simple to use.'}
    ], -- the array with the prompt history, and your new prompt
    {} -- An empty object of options (we're not specify additional options here)
) AS response;

---> give Snowflake Cortex Complete a prompt with a lengthier history
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-7b',
    [
        {'role': 'system',
        'content': 'Analyze this Snowflake review and determine the overall sentiment. Answer with just \"Positive\", \"Negative\", or \"Neutral\"' },
        {'role': 'user',
        'content': 'I love Snowflake because it is so simple to use.'},
        {'role': 'assistant',
        'content': 'Positive. The review expresses a positive sentiment towards Snowflake, specifically mentioning that it is \"so simple to use.\'"'},
        {'role': 'user',
        'content': 'Based on other information you know about Snowflake, explain why the reviewer might feel they way they do.'}
    ], -- the array with the prompt history, and your new prompt
    {} -- An empty object of options (we're not specify additional options here)
) AS response;


----------------------------------------------------------------------------------
--
----------------------------------------------------------------------------------
