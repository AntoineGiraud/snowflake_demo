import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

# make sure to define main when you’re working in a Python worksheet
def main(session: snowpark.Session):

    # load your table as a dataframe
    df_table = session.table("TASTY_BYTES.RAW_POS.MENU")
    # df_table = session.sql("SELECT * FROM TASTY_BYTES.RAW_POS.MENU LIMIT 10")
    df_table = df_table.filter(
        col("TRUCK_BRAND_NAME") == "The Mac Shack"
    ).select(col("MENU_ITEM_NAME"), col("ITEM_CATEGORY"))

    # execute the operations. (Remember, Snowpark DataFrames are evaluated lazily.)
    # df_table.show() # mais si on retourne la table ... il fera ça qd mm

    # save your dataframe as a table!
    df_table.write.save_as_table("TEST_DATABASE.TEST_SCHEMA.FREEZING_POINT_ITEMS", mode="append")

    # return your table
    return df_table

# ADDITIONAL IMPORTANT CODE SNIPPETS BELOW!

# you can run other commands through session.sql – even things like CREATE
    #session.sql("""
    #CREATE OR REPLACE TABLE TEST_DATABASE.TEST_SCHEMA.EMPTY_TABLE (
    #col1 varchar,
    #col2 varchar
    #)""").collect()
