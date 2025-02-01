/*--
In this Worksheet we will walk through creating a User in Snowflake.

For the User we will provide grants to a defined default role and default warehouse
and then walk through viewing all other users and roles in our account.

To conclude, we will drop the created User.
--*/


-------------------------------------------------------------------------------------------
    -- Step 1: To start, we first must set our Role context
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- System-Defined Roles: https://docs.snowflake.com/en/user-guide/security-access-control-overview#system-defined-roles
-------------------------------------------------------------------------------------------

--> To run a single query, place your cursor in the query editor and select the Run button (⌘-Return).
--> To run the entire worksheet, select 'Run All' from the dropdown next to the Run button (⌘-Shift-Return).

---> set our Role context
 USE ROLE USERADMIN;

-------------------------------------------------------------------------------------------
    -- Step 2: Create our User
        -- CREATE USER: https://docs.snowflake.com/en/sql-reference/sql/create-user
-------------------------------------------------------------------------------------------

---> now let's create a User using various available parameters.
    -- NOTE: please fill out each section below before executing the query

CREATE OR REPLACE USER dbt_runner -- adjust user name
    PASSWORD = 'xxxx' -- add a secure password
    LOGIN_NAME = 'dbt_runner' -- add a login name
    FIRST_NAME = 'DBT' -- add user's first name
    LAST_NAME = 'Runner' -- add user's last name
    EMAIL = 'dbt_runner@agiraud.com' -- add user's email
    MUST_CHANGE_PASSWORD = false -- ensures a password reset on first login
    DEFAULT_WAREHOUSE = COMPUTE_WH; -- set default warehouse to COMPUTE_WH

-- pour créer la clé : https://interworks.com/blog/2021/09/28/zero-to-snowflake-key-pair-authentication-with-windows-openssh-client/
-- soit ...
-- ssh-keygen -t rsa -b 2048 -m pkcs8 -C "dbtrunner_snow" -f key_dbtrunner_agiraud_snowflake
-- ssh-keygen -e -f .\key_dbtrunner_agiraud_snowflake.pub -m pkcs8
-- on on colle ci-après ... la clé publique encryptée
ALTER USER dbt_runner SET RSA_PUBLIC_KEY_2='3QIDAQAB';

show users;

CREATE ROLE dbt_runner;

/*--
With the User created, send the following information in a secure manner
to whomever the User is created for, so that they can access this Snowflake account:
  --> Snowflake Account URL: This is the Snowflake account link that they'll need to login. You can find this link at the top of your browser:(ex: https://app.snowflake.com/xxxxxxx/xxxxxxxx/)
  --> LOGIN_NAME: from above
  --> PASSWORD: from above
--*/

-------------------------------------------------------------------------------------------
    -- Step 3: Grant access to a Role and Warehouse for our User
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- GRANT ROLE: https://docs.snowflake.com/en/sql-reference/sql/grant-role
        -- GRANT <privileges>: https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
-------------------------------------------------------------------------------------------

---> with the User created, let's use our SECURITYADMIN role to grant the SYSADMIN role and COMPUTE_WH warehouse to it
USE ROLE SECURITYADMIN;

    /*--
      • Granting a role to another role creates a “parent-child” relationship between the roles (also referred to as a role hierarchy).
      • Granting a role to a user enables the user to perform all operations allowed by the role (through the access privileges granted to the role).

        NOTE: The SYSADMIN role has privileges to create warehouses, databases, and database objects in an account and grant those privileges to other roles.
        Only grant this role to Users who should have these privileges. You can view other system-defined roles in the documentation below:
            • https://docs.snowflake.com/en/user-guide/security-access-control-overview#label-access-control-overview-roles-system
    --*/

-- grant role SYSADMIN to our User
GRANT ROLE SYSADMIN TO USER dbt_runner;
REVOKE ROLE SYSADMIN FROM USER dbt_runner;
GRANT ROLE dbt_runner TO USER dbt_runner;


-- grant usage on the COMPUTE_WH warehouse to our SYSADMIN role
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DBT_RUNNER;

set dbt_role = 'dbt_runner';
set db_name = 'demo_dbt_jaffleshop';
select $db_name, $dbt_role;

grant all privileges on database identifier($db_name) to role identifier($dbt_role);
grant all privileges on ALL SCHEMAS IN DATABASE identifier($db_name) to role identifier($dbt_role);

GRANT USAGE ON DATABASE identifier($db_name) TO role identifier($dbt_role);
GRANT USAGE ON ALL SCHEMAS IN DATABASE identifier($db_name) TO ROLE identifier($dbt_role);
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE identifier($db_name) TO ROLE identifier($dbt_role);
GRANT SELECT ON ALL TABLES IN DATABASE identifier($db_name) TO ROLE identifier($dbt_role);
GRANT SELECT ON FUTURE TABLES IN DATABASE identifier($db_name) TO ROLE identifier($dbt_role);

-------------------------------------------------------------------------------------------
    -- Step 4: Explore all Users and Roles in our Account
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- SHOW USERS: https://docs.snowflake.com/en/sql-reference/sql/show-users
        -- SHOW ROLES: https://docs.snowflake.com/en/sql-reference/sql/show-roles
-------------------------------------------------------------------------------------------

---> let's now explore all users and roles in our account using our ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- show all users in account
SHOW USERS;

-- show all roles in account
SHOW ROLES;

-------------------------------------------------------------------------------------------
    -- Step 5: Drop our created Users
        -- DROP USER: https://docs.snowflake.com/en/sql-reference/sql/drop-user
-------------------------------------------------------------------------------------------

---> to drop the user, we could execute the following command
DROP USER <insert user name here>;
