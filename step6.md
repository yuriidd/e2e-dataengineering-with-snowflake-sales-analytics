Previous : [Step 5 - Foreign Exchange Rate Data](step5.md)

---

# 6 Loading Data From Internal Stage to Source Tables

# 6.1 Create Sequences

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.raw.in_sales_orders_seq
    START = 1
    INCREMENT = 1
    COMMENT = 'This is sequence for India Sales Orders Table.';

CREATE OR REPLACE SEQUENCE sales_dwh.raw.us_sales_orders_seq
    START = 1
    INCREMENT = 1
    COMMENT = 'This is sequence for USA Sales Orders Table.';

CREATE OR REPLACE SEQUENCE sales_dwh.raw.fr_sales_orders_seq
    START = 1
    INCREMENT = 1
    COMMENT = 'This is sequence for France Sales Orders Table.';
```

# 6.2 Create Sales Orders Tables

To columns from our file (csv etc) we will also add additional columns: 

- Sequence (as primary key)
- Source filename (as metadata)
- Source file row number (as metadata)
- Source file last modified timestamp (as metadata)

```sql
CREATE OR REPLACE TRANSIENT TABLE sales_dwh.raw.in_sales_orders (
    sales_order_key NUMBER(38,0),
    -- from csv
    order_id TEXT, 
    customer_name TEXT,
    mobile_model TEXT,
    quantity NUMBER(10, 0),
    unit_price NUMBER(10, 2),
    order_price NUMBER(10, 2),
    promotion_code TEXT,
    final_price NUMBER(10, 2),
    tax_amount TEXT,
    order_date DATE,
    payment_status TEXT,
    shipping_status TEXT,
    payment_method TEXT,
    payment_provider TEXT,
    phone_number TEXT,
    delivery_address TEXT,
    -- metadata
    _metadata_file_name TEXT,
    _metadata_row_number NUMBER(38,0),
    _metadata_last_modified TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TRANSIENT TABLE sales_dwh.raw.us_sales_orders (
    sales_order_key NUMBER(38,0),
    -- from parquet
    order_id TEXT, 
    customer_name TEXT,
    mobile_model TEXT,
    quantity NUMBER(10, 0),
    unit_price NUMBER(10, 2),
    order_price NUMBER(10, 2),
    promotion_code TEXT,
    final_price NUMBER(10, 2),
    tax_amount TEXT,
    order_date DATE,
    payment_status TEXT,
    shipping_status TEXT,
    payment_method TEXT,
    payment_provider TEXT,
    phone_number TEXT,
    delivery_address TEXT,
    -- metadata
    _metadata_file_name TEXT,
    _metadata_row_number NUMBER(38,0),
    _metadata_last_modified TIMESTAMP_NTZ(9)
);

CREATE OR REPLACE TRANSIENT TABLE sales_dwh.raw.fr_sales_orders (
    sales_order_key NUMBER(38,0),
    -- from json
    order_id TEXT, 
    customer_name TEXT,
    mobile_model TEXT,
    quantity NUMBER(10, 0),
    unit_price NUMBER(10, 2),
    order_price NUMBER(10, 2),
    promotion_code TEXT,
    final_price NUMBER(10, 2),
    tax_amount TEXT,
    order_date DATE,
    payment_status TEXT,
    shipping_status TEXT,
    payment_method TEXT,
    payment_provider TEXT,
    phone_number TEXT,
    delivery_address TEXT,
    -- metadata
    _metadata_file_name TEXT,
    _metadata_row_number NUMBER(38,0),
    _metadata_last_modified TIMESTAMP_NTZ(9)
);
```

## 6.3 Example of Loading Data From Staging to Raw Tables using Snowpark

Example uses Snowpark session to execute SQL code inside Snowflake. *You can also use this example to execute all the DDL code described above or earlier steps.*

See python code: [06-staging-to-raw-tables.py](06-staging-to-raw-tables.py)

**!!!** If you have an issue that there is no access to Tables, or File Format object, or staging when you execute python code - you should specify explicitly access for your object to `sysadmin` role. Same SQL code for loading data you can run in Snowsight (web) without error, but Snowpark API asks requires specify access explicit.

```sql
-- Tables
GRANT SELECT, INSERT ON TABLE sales_dwh.raw.in_sales_orders TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.raw.us_sales_orders TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.raw.fr_sales_orders TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.common.exchange_rates TO ROLE sysadmin;

-- Sequences
GRANT USAGE ON SEQUENCE sales_dwh.raw.in_sales_orders_seq TO ROLE sysadmin;
GRANT USAGE ON SEQUENCE sales_dwh.raw.us_sales_orders_seq TO ROLE sysadmin;
GRANT USAGE ON SEQUENCE sales_dwh.raw.fr_sales_orders_seq TO ROLE sysadmin;

-- File Formats
GRANT USAGE ON FILE FORMAT sales_dwh.common.SALES_CSV_FORMAT TO ROLE sysadmin; 
GRANT USAGE ON FILE FORMAT sales_dwh.common.SALES_PARQUET_FORMAT TO ROLE sysadmin; 
GRANT USAGE ON FILE FORMAT sales_dwh.common.SALES_JSON_FORMAT TO ROLE sysadmin; 

-- Staging
GRANT READ, WRITE ON STAGE sales_dwh.raw.raw_internal_stg TO ROLE sysadmin; 
```

---

Next : [Step 7 - Middle Layer of Transformations](step7.md)