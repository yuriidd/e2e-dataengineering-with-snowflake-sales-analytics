Previous: [Step 7 - Middle Layer of Transformations](step7.md)

---

# 8 Consumption Layer of Transformations

Consumption is a final layer, where tables for analysis are located. Consumers, analytics, BI systems etc connect to database and use these tables.

## 8.1 Creating Tables

#### `dim_region` table

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.consumption.dim_regions_seq 
    START = 1 
    INCREMENT = 1;

CREATE OR REPLACE TABLE sales_dwh.consumption.dim_regions (
    region_id NUMBER PRIMARY KEY,
    region TEXT,
    country TEXT,
    is_active TEXT(1)
);
```

#### `dim_products` table

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.consumption.dim_products_seq 
    START = 1 
    INCREMENT = 1;

SELECT OR REPLACE TABLE sales_dwh.consumption.dim_products (
    product_id NUMBER PRIMARY KEY,
    mobile_model TEXT,
    brand TEXT,
    model TEXT,
    color TEXT,
    memory TEXT,
    is_active TEXT(1)
);
```

#### `dim_promocodes` table

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.consumption.dim_promocodes_seq 
    START = 1 
    INCREMENT = 1;

CREATE OR REPLACE TABLE sales_dwh.consumption.dim_promocodes (
    promocode_id NUMBER PRIMARY KEY,
    promocode TEXT,
    is_active TEXT(1)
);
```

#### `dim_customers` table

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.consumption.dim_customers_seq 
    START = 1 
    INCREMENT = 1;

CREATE OR REPLACE TABLE sales_dwh.consumption.dim_customers (
    customer_id NUMBER PRIMARY KEY,
    customer_name TEXT,
    customer_phone_number TEXT,
    delivery_address TEXT,
    country TEXT,
    region TEXT,
    is_active TEXT(1)
);
```

#### `dim_payments` table

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.consumption.dim_payments_seq 
    START = 1 
    INCREMENT = 1;
    
CREATE OR REPLACE TABLE sales_dwh.consumption.dim_payments (
    payment_id NUMBER PRIMARY KEY,
    payment_method TEXT,
    payment_provider TEXT,
    currency TEXT,
    country TEXT,
    region TEXT,    
    is_active TEXT(1)
);
```

#### `dim_dates` table

I made next Calendar Table and created separate git repository [# Building an Advanced Date Calendar in Snowflake](https://github.com/yuriidd/advanced-date-calendar-snowflake). 

```sql
-- Create Table
CREATE OR REPLACE TABLE sales_dwh.consumption.dim_dates (
    date_id INT PRIMARY KEY,
    date DATE NOT NULL,
    epoch INT NOT NULL,
    day_suffix VARCHAR(3) NOT NULL,
    day_suffix_us VARCHAR(3) NOT NULL,
    day_name VARCHAR(9) NOT NULL,
    day_name_abbr VARCHAR(3) NOT NULL,
    day_of_week INT NOT NULL,
    day_of_week_us INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_quarter INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_month INT NOT NULL,
    week_of_year INT NOT NULL,
    week_of_year_iso VARCHAR(10) NOT NULL,
    month_ INT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    month_name_abbr VARCHAR(3) NOT NULL,
    quarter_ INT NOT NULL,
    quarter_name VARCHAR(9) NOT NULL,
    year_ INT NOT NULL,
    start_of_week DATE NOT NULL,
    start_of_month DATE NOT NULL,
    start_of_midmonth DATE NOT NULL,
    start_of_quarter DATE NOT NULL,
    start_of_year DATE NOT NULL,
    end_of_week DATE NOT NULL,
    end_of_month DATE NOT NULL,
    end_of_quarter DATE NOT NULL,
    end_of_year DATE NOT NULL,
    yyyymm VARCHAR NOT NULL,
    yyyymmdd VARCHAR NOT NULL,
    "Year" VARCHAR NOT NULL,
    "Month" VARCHAR NOT NULL,
    "Quarter" VARCHAR NOT NULL,
    "Week Monday" VARCHAR NOT NULL,
    is_weekend INT NOT NULL
);

-- Insert Values
INSERT INTO sales_dwh.consumption.dim_dates
SELECT 
TO_CHAR(datum, 'yyyymmdd')::int AS date_id
, datum AS date
, EXTRACT(epoch FROM datum) AS epoch
, DECODE(
    EXTRACT(dayofweek FROM datum), 
    1, '1st',
    2, '2nd', 
    3, '3rd', 
    4, '4th',
    5, '5th',
    6, '6th',
    0, '7th'
) AS day_suffix
, DECODE(
    EXTRACT(dayofweek FROM datum), 
    0, '1st',
    1, '2nd', 
    2, '3rd', 
    3, '4th',
    4, '5th',
    5, '6th',
    6, '7th'
) AS day_suffix_us
, DECODE(
    EXTRACT(dayofweek FROM datum), 
    0, 'Sunday',
    1, 'Monday', 
    2, 'Tuesday', 
    3, 'Wednesday',
    4, 'Thursday',
    5, 'Friday',
    6, 'Saturday'
) AS day_name
, DAYNAME(datum) AS day_name_abbr
, EXTRACT(dayofweekiso FROM datum) AS day_of_week
, EXTRACT(dayofweek FROM datum) AS day_of_week_us
, EXTRACT(DAY FROM datum) AS day_of_month
, datum - DATE_TRUNC('quarter', datum)::date + 1 AS day_of_quarter
, EXTRACT(dayofyear FROM datum) AS day_of_year
, IFF(
    DAYOFMONTH(datum) = 1 
    AND DAYNAME(datum) = 'Sun', 
    0, 
    FLOOR((DAY(datum) + 6.1) / 7, 0)
) AS week_of_month
, EXTRACT(week FROM datum) AS week_of_year
, TO_CHAR(datum, 'YYYY') || '-W' 
    || IFF(
            LEN(EXTRACT(week FROM datum))::int = 1,  
            CAST('0' || EXTRACT(week FROM datum) AS TEXT),
            EXTRACT(week FROM datum)::text
        )
    || '-' || EXTRACT(dayofweekiso FROM datum) AS week_of_year_iso
, EXTRACT(MONTH FROM datum) AS month_
, TO_CHAR(datum, 'MMMM') AS month_name
, TO_CHAR(datum, 'MON') AS month_name_abbr
, EXTRACT(quarter FROM datum) AS quarter_
, CONCAT('Q', EXTRACT(quarter FROM datum)) AS quarter_name
, EXTRACT(YEAR FROM datum) AS year_
, DATE_TRUNC('week', datum)::date AS start_of_week
, DATE_TRUNC('MONTH', datum)::date AS start_of_month
, CASE 
    WHEN EXTRACT(DAY FROM datum) < 15 
    THEN DATE_TRUNC('month', datum)::date
    ELSE DATE_TRUNC('month', datum)::date + INTERVAL '14 days'
END AS start_of_midmonth 
, DATE_TRUNC('quarter', datum)::date AS start_of_quarter
, DATE_TRUNC('YEAR', datum)::date AS start_of_year
, (
    DATE_TRUNC('week', datum) 
    + INTERVAL '1 WEEK' 
    - INTERVAL '1 day'
)::date AS end_of_week
, (
    DATE_TRUNC('MONTH', datum) 
    + INTERVAL '1 MONTH' 
    - INTERVAL '1 day'
)::date AS end_of_month
, (
    DATE_TRUNC('quarter', datum) 
    + INTERVAL '3 MONTH' 
    - INTERVAL '1 day'
)::date AS end_of_quarter
, (
    DATE_TRUNC('YEAR', datum)::date 
    + INTERVAL '1 YEAR' 
    - INTERVAL '1 day'
)::date AS end_of_year
, TO_CHAR(datum, 'yyyymm') AS yyyymm
, TO_CHAR(datum, 'yyyymmdd') AS yyyymmdd
, EXTRACT(YEAR FROM datum) AS "Year"
, CONCAT(YEAR(datum), '-', TO_CHAR(datum, 'Mon')) AS "Month"
, CONCAT(YEAR(datum), '-Q', EXTRACT(quarter FROM datum)) AS "Quarter"
, DATE_TRUNC('week', datum)::date AS "Week Monday" 
, CASE 
    WHEN EXTRACT(dayofweekiso FROM datum) IN (6,7) 
    THEN 1 
    ELSE 0 
END AS is_weekend 
FROM (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01')::date AS datum
    FROM TABLE(GENERATOR(ROWCOUNT => 400))
) AS dates_series;
```

#### `fact_sales` table

```sql
-- fact_sales
CREATE OR REPLACE TABLE sales_dwh.consumption.fact_sales (
    order_id NUMBER (38, 0),
    order_code TEXT,
    date_id NUMBER (38, 0),
    customer_id NUMBER (38, 0),
    region_id NUMBER (38, 0),
    product_id NUMBER (38, 0),
    promocode_id NUMBER (38, 0),
    payment_id NUMBER (38, 0),
    quantity NUMBER(10, 0),
    sales_local NUMBER(10, 2),
    tax_amount_local TEXT,
    exhchange_rate NUMBER(18, 8),
    sales_usd NUMBER(25, 10),
    tax_amount_usd NUMBER(25, 10)
);
```

#### Constraints

```sql
ALTER TABLE sales_dwh.consumption.fact_sales 
    ADD CONSTRAINT fact_sales_date_id_fk 
    FOREIGN KEY (date_id) REFERENCES sales_dwh.consumption.dim_dates (date_id) NOT ENFORCED;

ALTER TABLE sales_dwh.consumption.fact_sales 
    ADD CONSTRAINT fact_sales_customer_id_fk 
    FOREIGN KEY (customer_id) REFERENCES sales_dwh.consumption.dim_customers (customer_id) NOT ENFORCED;

ALTER TABLE sales_dwh.consumption.fact_sales 
    ADD CONSTRAINT fact_sales_region_id_fk 
    FOREIGN KEY (region_id) REFERENCES sales_dwh.consumption.dim_regions (region_id) NOT ENFORCED;

ALTER TABLE sales_dwh.consumption.fact_sales 
    ADD CONSTRAINT fact_sales_product_id_fk 
    FOREIGN KEY (product_id) REFERENCES sales_dwh.consumption.dim_products (product_id) NOT ENFORCED;

ALTER TABLE sales_dwh.consumption.fact_sales 
    ADD CONSTRAINT fact_sales_promocode_id_fk 
    FOREIGN KEY (promocode_id) REFERENCES sales_dwh.consumption.dim_promocodes (promocode_id) NOT ENFORCED;

ALTER TABLE sales_dwh.consumption.fact_sales 
    ADD CONSTRAINT fact_sales_payment_id_fk 
    FOREIGN KEY (payment_id) REFERENCES sales_dwh.consumption.dim_payments (payment_id) NOT ENFORCED;
```

```sql
-- Tables
GRANT SELECT, INSERT ON TABLE sales_dwh.consumption.dim_dates TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.consumption.dim_regions TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.consumption.dim_products TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.consumption.dim_promocodes TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.consumption.dim_customers TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.consumption.dim_payments TO ROLE sysadmin;

-- Sequences
GRANT USAGE ON SEQUENCE sales_dwh.consumption.dim_regions_seq TO ROLE sysadmin;
GRANT USAGE ON SEQUENCE sales_dwh.consumption.dim_products_seq TO ROLE sysadmin;
GRANT USAGE ON SEQUENCE sales_dwh.consumption.dim_promocodes_seq TO ROLE sysadmin;
GRANT USAGE ON SEQUENCE sales_dwh.consumption.dim_customers_seq TO ROLE sysadmin;
GRANT USAGE ON SEQUENCE sales_dwh.consumption.dim_payments_seq TO ROLE sysadmin;
```

## 8.2 Transform To Consumption Layer using Snowpark Python

Code for transformations from Middle to Consumption Layer look here: [08-transf2cons.py](08-transf2cons.py).

---

Back to Start: [README](README.md)
