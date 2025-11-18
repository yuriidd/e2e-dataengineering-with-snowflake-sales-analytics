from snowflake.snowpark import Session
# import os
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


def loading_in_sales(session) -> None:
    '''Function loading India sales data from internal stage to raw tables.'''

    query = '''
        COPY INTO sales_dwh.raw.in_sales_orders
        FROM (
            SELECT
                sales_dwh.raw.in_sales_orders_seq.nextval AS sales_order_key,
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
                t.$16::TEXT AS delivery_address,
                METADATA$FILENAME AS stg_file_name,
                METADATA$FILE_ROW_NUMBER AS stg_row_numer,
                METADATA$FILE_LAST_MODIFIED AS stg_last_modified
            FROM @sales_dwh.raw.raw_internal_stg/sales/source=IN/format=csv/
                (FILE_FORMAT => 'sales_dwh.common.sales_csv_format') AS t
        ) ON_ERROR = 'Continue'
        ;
        '''
    session.sql(query).collect()


def loading_us_sales(session) -> None:
    '''Function loading USA sales data from internal stage to raw tables.'''

    query = '''
        COPY INTO sales_dwh.raw.us_sales_orders
        FROM (
            SELECT
                sales_dwh.raw.us_sales_orders_seq.nextval AS sales_order_key,
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
                $1:"Delivery Address"::TEXT AS delivery_address,
                METADATA$FILENAME AS stg_file_name,
                METADATA$FILE_ROW_NUMBER AS stg_row_numer,
                METADATA$FILE_LAST_MODIFIED AS stg_last_modified
            FROM @sales_dwh.raw.raw_internal_stg/sales/source=US/format=parquet/
                (FILE_FORMAT => 'sales_dwh.common.sales_parquet_format')
        ) ON_ERROR = 'Continue'
        ;
        '''
    session.sql(query).collect()


def loading_fr_sales(session) -> None:
    '''Function loading France sales data from internal stage to raw tables.'''

    query = '''
        COPY INTO sales_dwh.raw.fr_sales_orders
        FROM (
            SELECT
                sales_dwh.raw.fr_sales_orders_seq.nextval AS sales_order_key,
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
                $1:"Delivery Address"::TEXT AS delivery_address,
                METADATA$FILENAME AS stg_file_name,
                METADATA$FILE_ROW_NUMBER AS stg_row_numer,
                METADATA$FILE_LAST_MODIFIED AS stg_last_modified
            FROM @sales_dwh.raw.raw_internal_stg/sales/source=FR/format=json/
                (FILE_FORMAT => 'sales_dwh.common.sales_json_format')
        ) ON_ERROR = 'Continue'
        ;
    '''
    session.sql(query).collect()


def main():
    # Create Snowpark Session object
    session = get_snowpark_session()

    # Loading India sales data from internal stage to raw tables
    loading_in_sales(session)

    # Loading USA sales data from internal stage to raw tables
    loading_us_sales(session)

    # Loading France sales data from internal stage to raw tables
    loading_fr_sales(session)


if __name__ == '__main__':
    main()