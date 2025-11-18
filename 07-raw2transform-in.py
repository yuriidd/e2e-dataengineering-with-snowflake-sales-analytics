from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit

import sys
import logging
import json


# Initiate logging at INFO level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                    format='%(asctime)s; %(levelname)s; %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Snowpark Session
def get_snowpark_session() -> Session:

    # Read parameters from file
    connection_parameters = json.load(open('connection.json'))

    # Creating Snowflake Session object
    return Session.builder.configs(connection_parameters).create()   



def main():
    # Getting Snowpark Session
    session = get_snowpark_session()

    # Retriving India Sales:
    # SELECT * 
    # FROM sales_dwh.raw.in_sales_orders;
    sales_df = session.table("sales_dwh.raw.in_sales_orders")


    # Filtering Snowpark DataFrame
    # Getting only PAID and DELIVERED orders
    """
    SELECT * 
    FROM sales_dwh.raw.in_sales_orders
    WHERE TRUE
        AND payment_status = 'Paid'
        AND shipping_status = 'Delivered'
    ;
    """
    filtered_df = (
        sales_df
        .filter(col('payment_status') == 'Paid')
        .filter(col('shipping_status') == 'Delivered')
    )


    # Adding country and region columns
    country_sales_df = (
        filtered_df
        .with_column('country', lit('IN'))
        .with_column('region', lit('APAC'))
    )

    # Uncomment to preview a few rows
    # country_sales_df.show(3)


    # Retriving Exchange Rates data and join to main table
    rates_df = session.table("sales_dwh.common.exchange_rates")

    # Uncomment to preview a few rows
    # rates_df.show(3)


    # Join Sales data with Rates
    merged_df = country_sales_df.join(
        rates_df, 
        country_sales_df['order_date'] == rates_df['exchange_rate_date'], 
        join_type='left'
    )

    # Uncomment to preview a few rows
    # merged_df.show(3)


    # 
    df_to_load = merged_df.select(
        'sales_order_key',
        'order_id',
        'order_date',
        'customer_name',
        col('phone_number').alias('customer_phone_number'),
        'mobile_model',

        'payment_status',
        'payment_method',
        'payment_provider',
        'country',
        'region',
        lit('INR').alias('currency'),
        'delivery_address',
        'shipping_status',
        'promotion_code',

        'quantity',
        col('unit_price').alias('unit_price_local'),
        col('order_price').alias('order_price_local'),
        col('final_price').alias('final_price_local'),
        col('tax_amount').alias('tax_amount_local'),
        col('usd2inr').alias('exhchange_rate'),
        (col('final_price') / col('usd2inr')).alias('final_price_usd'),
        (col('tax_amount') / col('usd2inr')).alias('tax_amount_usd'), 
    )

    # Uncomment to preview a few rows
    # df_to_load.show(3)


    # Loading data to Database
    df_to_load.write.save_as_table(
        'sales_dwh.transform.in_sales_orders', mode='append'
    )


if __name__ == '__main__':
    main()