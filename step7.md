Previous: [Step 6 - Loading Data From Staging to Raw Tables](step6.md)

---

# 7 Middle Layer of Transformations

# 7.1 Creating Sequences

```sql
CREATE OR REPLACE SEQUENCE sales_dwh.transform.in_sales_orders_seq
    START = 1
    INCREMENT = 1
    COMMENT = 'This is sequence for India Sales Orders Table.';

CREATE OR REPLACE SEQUENCE sales_dwh.transform.us_sales_orders_seq
    START = 1
    INCREMENT = 1
    COMMENT = 'This is sequence for USA Sales Orders Table.';

CREATE OR REPLACE SEQUENCE sales_dwh.transform.fr_sales_orders_seq
    START = 1
    INCREMENT = 1
    COMMENT = 'This is sequence for France Sales Orders Table.';
```

# 7.2 Creating Tables

```sql
CREATE OR REPLACE TABLE sales_dwh.transform.in_sales_orders (
    sales_order_key NUMBER(38,0), 
    order_id TEXT,
    order_date DATE,
    customer_name TEXT,
    customer_phone_number TEXT,
    mobile_model TEXT,
    --
    payment_status TEXT,
    payment_method TEXT,
    payment_provider TEXT,
    country TEXT,
    region TEXT,
    currency TEXT,
    delivery_address TEXT,
    shipping_status TEXT,
    --
    promotion_code TEXT,
    quantity NUMBER(10, 0),
    unit_price_local NUMBER(10, 2),
    order_price_local NUMBER(10, 2),
    final_price_local NUMBER(10, 2),
    tax_amount_local TEXT,
    exhchange_rate NUMBER(18, 8),
    final_price_usd NUMBER(25, 10),
    tax_amount_usd NUMBER(25, 10)
);

CREATE OR REPLACE TABLE sales_dwh.transform.us_sales_orders (
    sales_order_key NUMBER(38,0), 
    order_id TEXT,
    order_date DATE,
    customer_name TEXT,
    customer_phone_number TEXT,
    mobile_model TEXT,
    --
    payment_status TEXT,
    payment_method TEXT,
    payment_provider TEXT,
    country TEXT,
    region TEXT,
    currency TEXT,
    delivery_address TEXT,
    shipping_status TEXT,
    --
    promotion_code TEXT,
    quantity NUMBER(10, 0),
    unit_price_local NUMBER(10, 2),
    order_price_local NUMBER(10, 2),
    final_price_local NUMBER(10, 2),
    tax_amount_local TEXT,
    exhchange_rate NUMBER(18, 8),
    final_price_usd NUMBER(25, 10),
    tax_amount_usd NUMBER(25, 10)
);

CREATE OR REPLACE TABLE sales_dwh.transform.fr_sales_orders (
    sales_order_key NUMBER(38,0), 
    order_id TEXT,
    order_date DATE,
    customer_name TEXT,
    customer_phone_number TEXT,
    mobile_model TEXT,
    --
    payment_status TEXT,
    payment_method TEXT,
    payment_provider TEXT,
    country TEXT,
    region TEXT,
    currency TEXT,
    delivery_address TEXT,
    shipping_status TEXT,
    --
    promotion_code TEXT,
    quantity NUMBER(10, 0),
    unit_price_local NUMBER(10, 2),
    order_price_local NUMBER(10, 2),
    final_price_local NUMBER(10, 2),
    tax_amount_local TEXT,
    exhchange_rate NUMBER(18, 8),
    final_price_usd NUMBER(25, 10),
    tax_amount_usd NUMBER(25, 10)
);
```

And grant access.

```sql
GRANT SELECT, INSERT ON TABLE sales_dwh.transform.in_sales_orders TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.transform.us_sales_orders TO ROLE sysadmin;
GRANT SELECT, INSERT ON TABLE sales_dwh.transform.fr_sales_orders TO ROLE sysadmin;
```

# 7.3 Raw To Transform Layer using Snowpark Python

Here are three files, transformations for every country:

[India](07-raw2transform-in.py)

[United States](07-raw2transofrm-us.py)

[France](07-raw2transform-fr.py)

---

Next: [Step 8 - Consumption Layer of Transformations](step8.md)