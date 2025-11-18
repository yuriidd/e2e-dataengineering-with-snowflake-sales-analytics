# End to End ETL Project: DataEngineering with Snowflake - Sales Analytics

#snowflake #snowpark #python #sql #etl #flatfiles

This is an end-to-end ETL project data flow, starting from handling local flat data files of Sales and finishing with data modeling at mart (consumption) layer, using tools like Snowflake, Snowpark API, python and SQL.

### Data Flow

![](_att/Pasted%20image%2020251118155041.png)

### Data Overview

Dataset presents Amazon Mobile Sales from three countries.

That's folder structure of data. Different file formats for each country.

```bash
sales-data
├── additional
│   └── exchange-rate-data.csv
├── source=FR
│   └── format=json
│       ├── date=2020-01-02
│       │   └── order-20200102.json
│       ├── date=2020-01-03
│       │   └── order-20200103.json
│       ├── date=2020-01-04
│       │   └── order-20200104.json
│       └── ...
├── source=IN
│   └── format=csv
│       ├── date=2020-01-01
│       │   └── order-20200101.csv
│       ├── date=2020-01-02
│       │   └── order-20200102.csv
│       ├── date=2020-01-03
│       │   └── order-20200103.csv
│       └── ...
└── source=US
    └── format=parquet
        ├── date=2020-01-01
        │   └── order-20200101.snappy.parquet
        ├── date=2020-01-02
        │   └── order-20200102.snappy.parquet
        └── ...
```

### Steps:

[Step 1 - Create user & DWH](step1.md)

[Step 2 - Database & Schema](step2.md)

[Step 3 - Loading Data to Internal Stage](step3.md)

[Step 4 - Querying Data in Staged Files](step4.md)

[Step 5 - Foreign Exchange Rate Data](step5.md)

[Step 6 - Loading Data From Staging to Raw Tables](step6.md)

[Step 7 - Middle Layer of Transformations](step7.md)

[Step 8 - Consumption Layer of Transformations](step8.md)