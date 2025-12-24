from snowflake.snowpark import Session, DataFrame , CaseExpr
from snowflake.snowpark.functions import col, lit , row_number, rank, split, cast, when, expr, min, max
from snowflake.snowpark.types import StructType, StringType, StructField, StringType,LongType,DecimalType,DateType,TimestampType
from snowflake.snowpark import Window

import sys
import logging
import json
import pandas as pd

# Initiate logging at INFO level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                    format='%(asctime)s; %(levelname)s; %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


# Snowpark Session
def get_snowpark_session() -> Session:

    # Read parameters from file
    connection_parameters = json.load(open('connection.json'))

    # Creating Snowflake Session object
    return Session.builder.configs(connection_parameters).create()   


def create_dim_regions(sales_df, session) -> None:
    # Part 1
    # Getting new data
    # Remove duplicatesd
    dim_regions = sales_df.groupBy(col("region"), col("country")).count()
    dim_regions = dim_regions.with_column('is_active', lit(1).cast('INT'))

    dim_regions = dim_regions.selectExpr(
        'sales_dwh.consumption.dim_regions_seq.nextval as region_id_pk',
        'region', 'country', 'is_active'
    )
    # Uncomment to preview a few rows
    dim_regions.show(5)


    # Part 2
    # Getting existing data
    dim_regions_old = session.sql("SELECT region, country FROM sales_dwh.consumption.dim_regions")
    # Uncomment to preview a few rows
    dim_regions_old.show()


    # Return rows from dim_regions that do not have a matching 
    # region & country in dim_regions_old
    dim_regions_inc = dim_regions.join(
        dim_regions_old,
        (dim_regions["region"] == dim_regions_old["region"])
        & (dim_regions["country"]  == dim_regions_old["country"]),
        join_type='leftanti'
    )
    # Uncomment to preview a few rows
    dim_regions_inc.show(5)


    # Inserting data if there are new rows
    insert_count = int(dim_regions_inc.count())
    if insert_count > 0:
        dim_regions_inc.write.save_as_table(
            'sales_dwh.consumption.dim_regions', mode='append')
        print('The Load operation has been completed.')
    else:
        print('Nothing to Load.')


def create_dim_products(sales_df, session) -> None:
    # Part 1
    # Getting new data
    dim_products = (
        sales_df
        .with_column('brand', split(col('mobile_model'), lit('/'))[0])
        .with_column('model', split(col('mobile_model'), lit('/'))[1])
        .with_column('color', split(col('mobile_model'), lit('/'))[2])
        .with_column('memory', split(col('mobile_model'), lit('/'))[3])
        .select(
            col('mobile_model'), 
            col('brand').cast(StringType()).as_('brand'), 
            col('model').cast(StringType()).as_('model'),  
            col('color').cast(StringType()).as_('color'),   
            col('memory').cast(StringType()).as_('memory')
        )
    )
    # Uncomment to preview a few rows
    # dim_products.show(3)

    # Remove duplicates
    dim_products = dim_products.groupBy(
        col('mobile_model'), col('brand'), col('model'), col('color'), col('memory')
        ).count()
    dim_products = dim_products.with_column('is_active', lit(1).cast('INT'))
    # Uncomment to preview a few rows
    dim_products.show()


    # Part 2
    # Getting existing data
    dim_products_old = session.sql("SELECT mobile_model, brand, model, color, memory FROM sales_dwh.consumption.dim_products")


    # Return rows from dim_products that do not have a matching in dim_products_old
    dim_products_merged = dim_products.join(
        dim_products_old,
        ['mobile_model', 'brand', 'model', 'color', 'memory'],
        join_type='leftanti'
    )
    # Uncomment to preview a few rows
    dim_products_merged.show(5)


    # Select columns for increment
    dim_products_inc = dim_products_merged.selectExpr(
        'sales_dwh.consumption.dim_products_seq.nextval as product_id',
        'mobile_model', 'brand', 'model', 'color', 'memory', 'is_active'
    )
    # Uncomment to preview a few rows
    dim_products_inc.show(5)

    # Inserting data if there are new rows
    insert_count = int(dim_products_inc.count())
    if insert_count > 0:
        dim_products_inc.write.save_as_table(
            'sales_dwh.consumption.dim_products', mode='append')
        print('The Load operation has been completed.')
    else:
        print('Nothing to Load.')


def create_dim_promocodes(sales_df, session) -> None:
    # Part 1
    # Getting new data
    dim_promocodes = sales_df.with_column('promotion_code', 
        expr("CASE WHEN promotion_code IS NULL THEN 'NA' ELSE promotion_code END"))
    
    # Getting unique values of promotion_code
    dim_promocodes = dim_promocodes.groupBy(['promotion_code', 'region', 'country']).count()
    dim_promocodes = dim_promocodes.with_column("is_active", lit(1).cast('INT'))
    # Uncomment to preview a few rows
    dim_promocodes.show(3)


    # Part 2
    # Getting existing data
    dim_promocodes_old = session.sql("SELECT promotion_code, region, country FROM sales_dwh.consumption.dim_promocodes")


    # Return rows from dim_promocodes that do not have a matching in dim_promocodes_old
    dim_promocodes_merged = dim_promocodes.join(
        dim_promocodes_old,
        ["promotion_code", "country", "region"],
        join_type='leftanti'
    )
    # Uncomment to preview a few rows
    dim_promocodes_merged.show(3)


    # Select columns for increment
    dim_promocodes_inc = dim_promocodes_merged.selectExpr(
        "sales_dwh.consumption.dim_promocodes_seq.nextval as promotion_code_id", 
        "promotion_code", "region", "country", "is_active") 

    # Inserting data if there are new rows
    insert_count = int(dim_promocodes_inc.count())
    if insert_count > 0:
        dim_promocodes_inc.write.save_as_table(
            'sales_dwh.consumption.dim_promocodes', mode='append')
        print('The Load operation has been completed.')
    else:
        print('Nothing to Load.')


def create_dim_customers(sales_df, session) -> None:
    # Part 1
    # Getting new data
    # Selecting unique values
    dim_customers = sales_df.groupBy(
        ['customer_name', 'customer_phone_number', 'delivery_address', 'country', 'region']
        ).count()

    dim_customers = (
        dim_customers
        .select(['customer_name', 'customer_phone_number', 'delivery_address', 'country', 'region'])
        .with_column('is_active', lit(1).cast('INT'))
    )
    # Uncomment to preview a few rows
    dim_customers.show(3)


    # Part 2
    # Getting existing data
    dim_customers_old = session.sql(
        "SELECT customer_name, customer_phone_number, delivery_address, country, region FROM sales_dwh.consumption.dim_customers"
    )

    # Return rows from dim_customers that do not have a matching in dim_customers_old
    dim_customers_merged = dim_customers.join(
        dim_customers_old,
        ['customer_name', 'customer_phone_number', 'delivery_address', 'country', 'region'],
        join_type='leftanti'
    )

    # Select columns for increment
    dim_customers_inc = dim_customers_merged.selectExpr(
        "sales_dwh.consumption.dim_customers_seq.nextval as customer_id",
        "customer_name", "customer_phone_number", "delivery_address", 
        "country", "region", "is_active"
    )
    # Uncomment to preview a few rows
    dim_customers_inc.show(3)

    # Inserting data if there are new rows
    insert_count = int(dim_customers_inc.count())
    if insert_count > 0:
        dim_customers_inc.write.save_as_table(
            'sales_dwh.consumption.dim_customers', mode='append')
        print('The Load operation has been completed.')
    else:
        print('Nothing to Load.')


def create_dim_payments(sales_df, session) -> None:
    # Part 1
    # Getting new data
    # Selecting unique values
    dim_payments = sales_df.groupBy(
        ['payment_method', 'payment_provider', 'currency', 'country', 'region']
        ).count()

    dim_payments = (
        dim_payments
        .select(['payment_method', 'payment_provider', 'currency', 'country', 'region'])
        .with_column("is_active", lit(1).cast('INT'))
    )
    # Uncomment to preview a few rows
    dim_payments.show(3)


    # Part 2
    # Getting existing data
    dim_payments_old = session.sql(
        "SELECT payment_method, payment_provider, currency, country, region FROM sales_dwh.consumption.dim_payments")

    # Return rows from dim_payments that do not have a matching in dim_payments_old
    dim_payments_merged = dim_payments.join(
        dim_payments_old,
        ['payment_method', 'payment_provider', 'currency', 'country', 'region'],
        join_type='leftanti'
    )

    # Select columns for increment
    dim_payments_inc = dim_payments_merged.selectExpr(
        "sales_dwh.consumption.dim_payments_seq.nextval as payment_id",
        "payment_method", "payment_provider", "currency", "country", "region", "is_active"
    ) 
    # Uncomment to preview a few rows
    dim_payments_inc.show(3)

    # Inserting data if there are new rows
    insert_count = int(dim_payments_inc.count())
    if insert_count > 0:
        dim_payments_inc.write.save_as_table(
            'sales_dwh.consumption.dim_payments', mode='append')
        print('The Load operation has been completed.')
    else:
        print('Nothing to Load.')



def main():
    # Getting Snowpark Session
    session = get_snowpark_session()

    in_sales_df = session.table("sales_dwh.transform.in_sales_orders")
    us_sales_df = session.table("sales_dwh.transform.us_sales_orders")
    fr_sales_df = session.table("sales_dwh.transform.fr_sales_orders")

    sales_df = in_sales_df.union(us_sales_df).union(fr_sales_df)
    # Uncomment to preview a few rows
    # sales_df.show(3)


    # Loading dimentional tables
    # dim_dates also created
    create_dim_regions(sales_df, session)
    create_dim_products(sales_df, session)
    create_dim_promocodes(sales_df, session)
    create_dim_customers(sales_df, session)
    create_dim_payments(sales_df, session)


    # Fetching dimentional tables
    dim_dates = session.sql(
        "SELECT date_id, date FROM sales_dwh.consumption.dim_dates")
    dim_regions = session.sql(
        "SELECT region_id, region, country FROM sales_dwh.consumption.dim_regions")
    dim_products = session.sql(
        "SELECT product_id, mobile_model FROM sales_dwh.consumption.dim_products")
    dim_promocodes = session.sql(
        "SELECT promotion_code_id, promotion_code, region, country FROM sales_dwh.consumption.dim_promocodes")
    dim_customers = session.sql(
        "SELECT customer_id, customer_name, delivery_address, country, region FROM sales_dwh.consumption.dim_customers")
    dim_payments = session.sql(
        "SELECT payment_id, payment_method, payment_provider, country, region FROM sales_dwh.consumption.dim_payments")


    # Loading fact table
    sales_tmp = sales_df.with_column("promotion_code", 
            expr("CASE WHEN promotion_code IS NULL THEN 'NA' ELSE promotion_code END"))
    sales_tmp = sales_tmp.join(dim_promocodes, ["promotion_code", "region", "country"], join_type='inner')

    sales_tmp = sales_tmp.join(dim_dates, sales_df['order_date'] == dim_dates['date'], join_type='inner')

    sales_tmp = sales_tmp.join(dim_regions, ["country", "region"], join_type='inner')
    sales_tmp = sales_tmp.join(dim_products, ["mobile_model"], join_type='inner')
    sales_tmp = sales_tmp.join(dim_customers, ["customer_name", "delivery_address", "country", "region"], join_type='inner')
    sales_tmp = sales_tmp.join(dim_payments, ["payment_method", "payment_provider", "country", "region"], join_type='inner')

    sales_merged = sales_tmp
    # Uncomment to preview a few rows
    sales_merged.show(3)

    sales_inc = sales_merged.selectExpr("""
        sales_dwh.consumption.fact_sales_seq.nextval AS order_id,
        order_id AS order_code,
        date_id,
        customer_id,
        region_id,
        product_id,
        promotion_code_id,
        payment_id,
        quantity,
        final_price_local AS sales_local,
        tax_amount_local,
        exhchange_rate,
        final_price_usd AS sales_usd,
        tax_amount_usd
    """)
    # Uncomment to preview a few rows
    # sales_inc.show(5)


    # Inserting data to fact_sales
    sales_inc.write.save_as_table("sales_dwh.consumption.fact_sales", mode="append")
    print('Final Load operation has been completed.')



if __name__ == '__main__':
    main()
