Previous : [Step 2 - Database & Schema](step2.md)

---

# 3.1 Loading Data To Internal Stage Using Snowpark File API

Create and fill the file `connection.json` with you credentials. `account` is your Snowflake's Account identifier.

```json
{
	"account" : "<YOURSNOWFLAKE-ACCOUNT>",
	"user" : "snowpark_user",
	"password" : "@OLOLO#789",
	"role" : "SYSADMIN",
	"warehouse" : "SNOWPARK_ETL_WH",
	"database" : "SNOWFLAKE_SAMPLE_DATA",
	"schema" : "TPCH_SF1"
}
```

Open [03-load-to-staging](03-load-to-staging.py), change `directory_to_search` variable to directory, where you want to search for files. 

Script searches for `.csv`, `.json` and `.parquet` files and then push them to Snowflake stage.

```bash
python3 03-load-to-staging.py
```

The result you can see by executing next command:

```sql
USE SCHEMA raw;
LIST @raw_internal_stg;
```

![](_att/Pasted%20image%2020251110213706.png)

---

Next : [Step 4 - Querying Data in Staged Files](step4.md)