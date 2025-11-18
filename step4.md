Previous : [Step 3 - Loading Data to Internal Stage](step3.md)

---

# 4. 1 File Format Objects

To read data from the internal stage we should create File Format Object. We will create it in `common` schema.

```sql
USE SCHEMA common;

-- Create File Format CSV (for India data)
CREATE OR REPLACE FILE FORMAT sales_csv_format
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 1
NULL_IF = ('null', 'null')
EMPTY_FIELD_AS_NULL = TRUE
FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
COMPRESSION = AUTO;


-- Create File Format JSON (for France data)
CREATE OR REPLACE FILE FORMAT sales_json_format
TYPE = JSON
STRIP_OUTER_ARRAY = TRUE
COMPRESSION = AUTO;


-- Create File Format PARQUET (for USA data)
CREATE OR REPLACE FILE FORMAT parquet_csv_format
TYPE = PARQUET
COMPRESSION = SNAPPY;
```


# 4.2 Querying Data in Staged Files

CSV's and others are loaded to Stage. Now let's query that data like sql table and see what the data looks like.

```sql
USE DATABASE sales_dwh;
USE SCHEMA raw;
```

For CSV's:

```sql
SELECT
    t.$1::TEXT AS order_id, 
    t.$2::TEXT AS customer_name,
    t.$3::TEXT AS mobile_model,
    t.$4::NUMBER(10, 0) AS quantity,
    t.$5::NUMBER(10, 2) AS unit_price,
    t.$6::NUMBER(10, 2) AS order_price,
    t.$7::TEXT AS promotion_code,
    t.$8::NUMBER(10, 2) AS final_price,
    t.$9::TEXT AS tax_amount,
    t.$10::DATE AS order_date,
    t.$11::TEXT AS payment_status,
    t.$12::TEXT AS shipping_status,
    t.$13::TEXT AS payment_method,
    t.$14::TEXT AS payment_provider,
    t.$15::TEXT AS phone_number,
    t.$16::TEXT AS delivery_address
FROM @raw_internal_stg/sales/source=IN/format=csv/
    (FILE_FORMAT => 'sales_dwh.common.sales_csv_format') AS t
;
```

For PARQUET's:

```sql
SELECT
    $1:"Order ID"::TEXT AS order_id, 
    $1:"Customer Name"::TEXT AS customer_name,
    $1:"Mobile Model"::TEXT AS mobile_model,
    $1:"Quantity"::NUMBER(10, 0) AS quantity,
    $1:"Price per Unit"::NUMBER(10, 2) AS unit_price,
    $1:"Total Price"::NUMBER(10, 2) AS order_price,
    $1:"Promotion Code"::TEXT AS promotion_code,
    $1:"Order Amount"::NUMBER(10, 2) AS final_price,
    $1:"Tax"::NUMBER(10, 2) AS tax_amount,
    $1:"Order Date"::DATE AS order_date,
    $1:"Payment Status"::TEXT AS payment_status,
    $1:"Shipping Status"::TEXT AS shipping_status,
    $1:"Payment Method"::TEXT AS payment_method,
    $1:"Payment Provider"::TEXT AS payment_provider,
    $1:"Phone"::TEXT AS phone_number,
    $1:"Delivery Address"::TEXT AS delivery_address
FROM @raw_internal_stg/sales/source=US/format=parquet/
    (FILE_FORMAT => 'sales_dwh.common.sales_parquet_format')
;
```


For JSON's:

```sql
SELECT
    $1:"Order ID"::TEXT AS order_id, 
    $1:"Customer Name"::TEXT AS customer_name,
    $1:"Mobile Model"::TEXT AS mobile_model,
    $1:"Quantity"::NUMBER(10, 0) AS quantity,
    $1:"Price per Unit"::NUMBER(10, 2) AS unit_price,
    $1:"Total Price"::NUMBER(10, 2) AS order_price,
    $1:"Promotion Code"::TEXT AS promotion_code,
    $1:"Order Amount"::NUMBER(10, 2) AS final_price,
    $1:"Tax"::NUMBER(10, 2) AS tax_amount,
    $1:"Order Date"::DATE AS order_date,
    $1:"Payment Status"::TEXT AS payment_status,
    $1:"Shipping Status"::TEXT AS shipping_status,
    $1:"Payment Method"::TEXT AS payment_method,
    $1:"Payment Provider"::TEXT AS payment_provider,
    $1:"Phone"::TEXT AS phone_number,
    $1:"Delivery Address"::TEXT AS delivery_address
FROM @raw_internal_stg/sales/source=FR/format=json/
    (FILE_FORMAT => 'sales_dwh.common.sales_json_format')
;
```

---

Next : [Step 5 - Foreign Exchange Rate Data](step5.md)