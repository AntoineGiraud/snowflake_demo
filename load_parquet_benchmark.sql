

 USE ROLE USERADMIN;

CREATE OR REPLACE USER loader -- adjust user name
    PASSWORD = 'xxxx' -- add a secure password
    LOGIN_NAME = 'loader' -- add a login name
    FIRST_NAME = 'loader' -- add user's first name
    LAST_NAME = 'loader' -- add user's last name
    EMAIL = 'loader@agiraud.com' -- add user's email
    MUST_CHANGE_PASSWORD = false -- ensures a password reset on first login
    DEFAULT_WAREHOUSE = COMPUTE_WH; -- set default warehouse to COMPUTE_WH

-- pour créer la clé : https://interworks.com/blog/2021/09/28/zero-to-snowflake-key-pair-authentication-with-windows-openssh-client/
-- soit ...
-- ssh-keygen -t rsa -b 2048 -m pkcs8 -C "agiraud_snow" -f key_agiraud_snowflake
-- ssh-keygen -e -f .\key_agiraud_snowflake.pub -m pkcs8
-- on on colle ci-après ... la clé publique encryptée
ALTER USER loader SET RSA_PUBLIC_KEY_2='3QIDAQAB';

show users;