Previous : [Step 4 - Querying Data in Staged Files](step4.md)

---

# 5.1 Foreign Exchange Rate Data

 [Exchange Rate Data](sales-data/exchange-rate-data.csv) can be used for presenting KPIs from different regions in US dollars and for comparison purposes.

Lets load that data to `common` schema.

```sql
LIST @sales_dwh.raw.raw_internal_stg/sales/additional/

CREATE OR REPLACE TRANSIENT TABLE sales_dwh.common.exchange_rates (
    exchange_rate_date DATE,
    usd2usd NUMBER(12, 6),
    usd2eu NUMBER(12, 6),
    usd2can NUMBER(12, 6),
    usd2uk NUMBER(12, 6),
    usd2inr NUMBER(12, 6),
    usd2jp NUMBER(12, 6)
);


COPY INTO sales_dwh.common.exchange_rates
FROM (
    SELECT
        t.$1::DATE AS exchange_rate_date,
        t.$2::NUMBER(12, 6) AS usd2usd,
        t.$3::NUMBER(12, 6) AS usd2eu,
        t.$4::NUMBER(12, 6) AS usd2can,
        t.$2::NUMBER(12, 6) AS usd2uk,
        t.$5::NUMBER(12, 6) AS usd2inr,
        t.$6::NUMBER(12, 6) AS usd2jp
    FROM @sales_dwh.raw.raw_internal_stg/sales/additional/exchange-rate-data.csv
        (FILE_FORMAT => 'sales_dwh.common.sales_csv_format') AS t
)
;
```

The result:

![](_att/Pasted%20image%2020251111150933.png)

---

Next: [Step 6 - Loading Data From Staging to Raw Tables](step6.md)