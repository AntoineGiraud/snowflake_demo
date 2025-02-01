import os, yaml
import snowflake.connector
from time import time

config = yaml.safe_load(open("conf.yml"))
snow = config["snow"]

conn_snow = snowflake.connector.connect(
    user=snow["user"],
    # password = snow['password'],
    private_key_file=snow["private_key_path"],
    account=snow["account"],
    role=snow["role"],
    warehouse=snow["warehouse"],
    database=snow["database"],
    schema=snow["schema"],
)


def put_files_to_snowflake_stage(conn_snow, file_name, stage_name):
    cs = conn_snow.cursor()
    time_step = time()
    try:
        files = cs.execute(
            f"PUT file://{file_name} @{stage_name} auto_compress=false"
        ).fetchall()
        files_loaded = [f for f in files if f[6] != "SKIPPED"]
        print(
            f"{len(files_loaded)} / {len(files)} files loaded ({round(time() - time_step, 2)}s)"
        )
    finally:
        cs.close()


put_files_to_snowflake_stage(
    conn_snow,
    "data/*.parquet",
    f"{snow['database']}.{snow['schema']}.{snow['stage']}",
)

# inspi : https://medium.com/@srikanth-/snowflake-load-multiple-files-into-internal-stage-using-python-code-6d30fe1de426
