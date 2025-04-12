# ❄ snowflake demo project

## bikeshare project

### Init gouvernance (role, usr, schemas, warehouse, grants)

Here is the roles implemented by `bikeshare_init_roleSchemasGrantsUser.sql`

- bikeshare_**admin** * - granted to sysadmin*
  - bikeshare_**loader**       <br>
      uses 💻 `bikeshare_loading_wh`<br>
      owns 🥉 bonze schemas
  - bikeshare_**transformer**<br>
      uses 💻 `bikeshare_transforming_wh`<br>
      owns 🥈 silver & 🥇 gold schemas
    - bikeshare_**reader**<br>
      using 💻 `bikeshare_reading_wh`<br>
      reads all schemas 🥉🥈🥇

![bikeshare_roles_and_schemas](./bikeshare/bikeshare_roles_and_schemas.png)

inspirations
- dbt [how we use snowflake](https://discourse.getdbt.com/t/setting-up-snowflake-the-exact-grant-statements-we-run/439)

![snowflake_load_perf](./bikeshare/snowflake_load_perf.png)