Previous : [Step 1 - Create user & DWH](step1.md)

---

# 2.1 Create Database & Schemas

![](_att/Pasted%20image%2020251118155041.png)


```sql
-- Create Database
CREATE DATABASE IF NOT EXISTS sales_dwh;

-- Create Schemas
USE DATABASE sales_dwh;
CREATE SCHEMA IF NOT EXISTS raw;    -- Extracted data from sources
CREATE SCHEMA IF NOT EXISTS transform;   -- Cleaning, deduplication etc
CREATE SCHEMA IF NOT EXISTS consumption; -- Fact and dimention tables
CREATE SCHEMA IF NOT EXISTS audit;  -- To capture audit recoreds
CREATE SCHEMA IF NOT EXISTS common; -- For file formats sequence object etc
```

# 2.2 Create Internal Stage

Data from sources will be put at stage within `raw` schema.

```sql
-- Create Internal Stage within raw Schema 
USE SCHEMA raw;  
CREATE OR REPLACE STAGE raw_internal_stg;
```

P.S. Try use the notations like **`database.schema.table`** which is called a **fully‑qualified (object) name** (or **three‑part identifier**) in your code to aviod errors. 

---

Next :  [Step 3 - Loading Data to Internal Stage](step3.md)