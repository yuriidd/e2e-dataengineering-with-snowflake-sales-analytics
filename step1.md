To the beginning: [README](README.md)

---

# 1.1 Create user & DWH

SnowsightUI: Projects > Worksheets > *New SQL Worksheet*

```sql
-- Create a Virtual Warehouse
USE ROLE sysadmin;
CREATE OR REPLACE WAREHOUSE snowpark_etl_wh
	WITH
	WAREHOUSE_TYPE = STANDARD
	WAREHOUSE_SIZE = XSMALL
	AUTO_SUSPEND = 60
	AUTO_RESUME = TRUE
	MAX_CLUSTER_COUNT = 1
	MIN_CLUSTER_COUNT = 1
	SCALING_POLICY = STANDARD
;

-- Create Snowpark User
USE ROLE accountadmin;  -- Only accound admin can create that user
CREATE USER snowpark_user
PASSWORD = '@OLOLO#789'
DEFAULT_ROLE = sysadmin
DEFAULT_SECONDARY_ROLES = ( 'ALL' )
MUST_CHANGE_PASSWORD = FALSE
COMMENT = 'This is a Snowpark User'
;

-- Grant Roles
GRANT ROLE sysadmin to USER snowpark_user;
GRANT USAGE ON WAREHOUSE snowpark_etl_wh TO ROLE sysadmin;
```

---

# 1.2 Snowpark Snowflake Connectivity Validation

Create and fill the file `connection.json` with new database `SALES_DWH`. `account` is your Snowflake's Account identifier.

```json
{
	"account" : "<YOURSNOWFLAKE-ACCOUNT>",
	"user" : "snowpark_user",
	"password" : "@OLOLO#789",
	"role" : "SYSADMIN",
	"warehouse" : "SNOWPARK_ETL_WH",
	"database" : "SALES_DWH",
	"schema" : "RAW"
}
```

Run script [`00-connection-validation.py`](00-connection-validation.py). It creates Snowpark Session and perform 2 simple queries.

```bash
python3 00-connection-validation.py
```

The result:

![](_att/Pasted%20image%2020251107215036.png)

---

Next : [Step 2 - Database & Schema](step2.md)