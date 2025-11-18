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













# # This is a simple dim table having nation and region.
# # fields are 'Country','Region'
# def create_region_dim(all_sales_df, session)-> None:
#     region_dim_df = all_sales_df.groupBy(col("Country"),col("Region")).count()
#     region_dim_df.show(2)
#     region_dim_df = region_dim_df.with_column("isActive",lit('Y'))
#     region_dim_df = region_dim_df.selectExpr("sales_dwh.source.region_dim_seq.nextval as region_id_pk","Country", "Region", "isActive") 
#     #region_dim_df.write.save_as_table('sales_dwh.consumption.region_dim',mode="append")   

#     region_dim_df.show(5)
#     # part 2 where delta data will be processed 
    
#     existing_region_dim_df = session.sql("select Country, Region from sales_dwh.consumption.region_dim")

#     region_dim_df = region_dim_df.join(existing_region_dim_df,region_dim_df['Country']==existing_region_dim_df['Country'],join_type='leftanti')
#     region_dim_df.show(5)
#     intsert_cnt = int(region_dim_df.count())
#     if intsert_cnt>0:
#         region_dim_df.write.save_as_table("sales_dwh.consumption.region_dim",mode="append")
#         print("save operation ran...")
#     else:
#         print("No insert ...Opps...")


#     # have exclude key


# def create_product_dim(all_sales_df, session)-> None:

#     product_dim_df = all_sales_df.with_column("Brand",split(col('MOBILE_KEY'),lit('/'))[0])     \
#                                 .with_column("Model",split(col('MOBILE_KEY'),lit('/'))[1])      \
#                                 .with_column("Color",split(col('MOBILE_KEY'),lit('/'))[2])      \
#                                 .with_column("Memory",split(col('MOBILE_KEY'),lit('/'))[3])     \
#                                 .select(col('mobile_key'),col('Brand'),col('Model'),col('Color'),col('Memory'))
    
#     product_dim_df = product_dim_df.select(col('mobile_key'),                    \
#                                         cast(col('Brand'), StringType()).as_("Brand"),\
#                                         cast(col('Model'), StringType()).as_("Model"),\
#                                         cast(col('Color'), StringType()).as_("Color"),\
#                                         cast(col('Memory'), StringType()).as_("Memory")\
#                                         )
    
#     product_dim_df = product_dim_df.groupBy(col('mobile_key'),col("Brand"),col("Model"),col("Color"),col("Memory")).count()
#     product_dim_df = product_dim_df.with_column("isActive",lit('Y'))

#     #fetch existing product dim records.
#     existing_product_dim_df = session.sql("select mobile_key, Brand, Model, Color, Memory from sales_dwh.consumption.product_dim")
#     existing_product_dim_df.count()

#     product_dim_df = product_dim_df.join(existing_product_dim_df,["mobile_key", "Brand", "Model", "Color", "Memory"],join_type='leftanti')

#     product_dim_df.show(5)

#     product_dim_df = product_dim_df.selectExpr("sales_dwh.consumption.product_dim_seq.nextval as product_id_pk","mobile_key","Brand", "Model","Color","Memory", "isActive") 

#     product_dim_df.show(5)
#     intsert_cnt = int(product_dim_df.count())
#     if intsert_cnt>0:
#         product_dim_df.write.save_as_table("sales_dwh.consumption.product_dim",mode="append")
#         print("save operation ran...")
#     else:
#         print("No insert ...Opps...")





# def create_promocode_dim(all_sales_df,session)-> None:


#     promo_code_dim_df = all_sales_df.with_column( "promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
#     promo_code_dim_df = promo_code_dim_df.groupBy(col("promotion_code"),col("country"),col("region")).count()
#     promo_code_dim_df = promo_code_dim_df.with_column("isActive",lit('Y'))

#     #promo_code_dim_df.show(10)

    
#     #fetch existing product dim records.
#     existing_promo_code_dim_df = session.sql("select promotion_code, country, region from sales_dwh.consumption.promo_code_dim")


#     promo_code_dim_df = promo_code_dim_df.join(existing_promo_code_dim_df,["promotion_code", "country", "region"],join_type='leftanti')


#     promo_code_dim_df = promo_code_dim_df.selectExpr("sales_dwh.consumption.promo_code_dim_seq.nextval as promo_code_id_pk","promotion_code", "country","region","isActive") 


#     intsert_cnt = int(promo_code_dim_df.count())
#     if intsert_cnt>0:
#         promo_code_dim_df.write.save_as_table("sales_dwh.consumption.promo_code_dim",mode="append")
#         print("save operation ran...")
#     else:
#         print("No insert ...Opps...")
    



# def create_customer_dim(all_sales_df, session) -> None:
#     customer_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("CUSTOMER_NAME"),col("CONCTACT_NO"),col("SHIPPING_ADDRESS")).count()
#     customer_dim_df = customer_dim_df.with_column("isActive",lit('Y'))
#     customer_dim_df = customer_dim_df.selectExpr("customer_name", "conctact_no","shipping_address","country","region" ,"isactive") 
#     #region_dim_df.write.save_as_table('sales_dwh.consumption.region_dim',mode="append")   

#     customer_dim_df.show(5)
#     # part 2 where delta data will be processed 
    
#     existing_customer_dim_df = session.sql("select customer_name,conctact_no,shipping_address,country, region from sales_dwh.consumption.customer_dim")

#     customer_dim_df = customer_dim_df.join(existing_customer_dim_df,["customer_name","conctact_no","shipping_address","country", "region"],join_type='leftanti')

#     customer_dim_df = customer_dim_df.selectExpr("sales_dwh.consumption.customer_dim_seq.nextval as customer_id_pk","customer_name", "conctact_no","shipping_address","country","region", "isActive") 

#     customer_dim_df.show(5)

#     intsert_cnt = int(customer_dim_df.count())
#     if intsert_cnt>0:
#         customer_dim_df.write.save_as_table("sales_dwh.consumption.customer_dim",mode="append")
#         print("save operation ran...")
#     else:
#         print("No insert ...Opps...")
    
# def create_payment_dim(all_sales_df, session) -> None:
#     payment_dim_df = all_sales_df.groupBy(col("COUNTRY"),col("REGION"),col("payment_method"),col("payment_provider")).count()
#     payment_dim_df = payment_dim_df.with_column("isActive",lit('Y'))

#     #region_dim_df.write.save_as_table('sales_dwh.consumption.region_dim',mode="append")   

#     payment_dim_df.show(5)
#     # part 2 where delta data will be processed 
    
#     existing_payment_dim_df = session.sql("select payment_method,payment_provider,country, region from sales_dwh.consumption.payment_dim")

#     payment_dim_df = payment_dim_df.join(existing_payment_dim_df,["payment_method","payment_provider","country", "region"],join_type='leftanti')

#     payment_dim_df = payment_dim_df.selectExpr("sales_dwh.consumption.payment_dim_seq.nextval as payment_id_pk","payment_method", "payment_provider","country","region", "isActive") 


#     intsert_cnt = int(payment_dim_df.count())
#     if intsert_cnt>0:
#         payment_dim_df.write.save_as_table("sales_dwh.consumption.payment_dim",mode="append")
#         print("save operation ran...")
#     else:
#         print("No insert ...Opps...")

# def create_date_dim(all_sales_df, session) -> None:
#     start_date = all_sales_df.select(min("order_dt").alias("min_order_dt")).collect()[0].as_dict()['MIN_ORDER_DT']
#     end_date = all_sales_df.select(max("order_dt").alias("max_order_dt")).collect()[0].as_dict()['MAX_ORDER_DT']
#     date_range = pd.date_range(start=start_date, end=end_date, freq='D')
#     #print(date_range)
#     date_dim = pd.DataFrame()
#     date_dim['order_dt'] = date_range.date
#     date_dim['Year'] = date_range.year
#     # Calculate day counter
#     start_day_of_year = pd.to_datetime(start_date).dayofyear
#     date_dim['DayCounter'] = date_range.dayofyear - start_day_of_year + 1

#     date_dim['Month'] = date_range.month
#     date_dim['Quarter'] = date_range.quarter
#     date_dim['Day'] = date_range.day
#     date_dim['DayOfWeek'] = date_range.dayofweek
#     date_dim['DayName'] = date_range.strftime('%A')
#     date_dim['DayOfMonth'] = date_range.day
#     date_dim['Weekday'] = date_dim['DayOfWeek'].map({0: 'Weekday', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Weekend', 6: 'Weekend'})


#     date_dim_df = session.create_dataframe(date_dim)

#     existing_date_dim_df = session.sql("select order_dt from sales_dwh.consumption.date_dim ") 
#     date_dim_df = date_dim_df.join(existing_date_dim_df,existing_date_dim_df['order_dt']==date_dim_df['"order_dt"'],join_type='leftanti')

#     date_dim_df = date_dim_df.selectExpr(' \
#                                    sales_dwh.consumption.date_dim_seq.nextval, \
#                                    "order_dt" as order_dt, \
#                                    "DayCounter" as day_counter,\
#                                    "Year" as order_year, \
#                                    "Month" as order_month, \
#                                    "Quarter" as order_quarter, \
#                                    "Day" as order_day, \
#                                    "DayOfWeek" as order_dayofweek, \
#                                    "DayName" as order_dayname, \
#                                    "DayOfMonth" as order_dayofmonth, \
#                                    "Weekday" as order_weekday\
#                                    ')

#     intsert_cnt = int(date_dim_df.count())
#     if intsert_cnt>0:
#         date_dim_df.write.save_as_table("sales_dwh.consumption.date_dim",mode="append")
#         print("save operation ran...")
#     else:
#         print("No insert ...Opps...")

# def main():
#     #get the session object and get dataframe
#     session = get_snowpark_session()

#     in_sales_df = session.sql("select * from sales_dwh.curated.in_sales_order")
#     us_sales_df = session.sql("select * from sales_dwh.curated.us_sales_order")
#     fr_sales_df = session.sql("select * from sales_dwh.curated.fr_sales_order")

#     all_sales_df = in_sales_df.union(us_sales_df).union(fr_sales_df)

#     create_date_dim(all_sales_df,session)       #date dimension


#     create_region_dim(all_sales_df,session)     #region dimension
#     create_product_dim(all_sales_df,session)    #product dimension
#     create_promocode_dim(all_sales_df,session)  #promot code dimension
#     create_customer_dim(all_sales_df,session)   #customer dimension
#     create_payment_dim(all_sales_df,session)    #payment dimension
#     create_date_dim(all_sales_df,session)       #date dimension


#     date_dim_df = session.sql("select date_id_pk, order_dt from sales_dwh.consumption.date_dim")
#     customer_dim_df = session.sql(
#         "select customer_id_pk, customer_name, country, region from sales_dwh.consumption.CUSTOMER_DIM")
#     payment_dim_df = session.sql(
#         "select payment_id_pk, payment_method, payment_provider, country, region from sales_dwh.consumption.PAYMENT_DIM")
#     product_dim_df = session.sql(
#         "select product_id_pk, mobile_key from sales_dwh.consumption.PRODUCT_DIM")
#     promo_code_dim_df = session.sql(
#         "select promo_code_id_pk,promotion_code,country, region from sales_dwh.consumption.PROMO_CODE_DIM")
#     region_dim_df = session.sql(
#         "select region_id_pk,country, region from sales_dwh.consumption.REGION_DIM")





#     all_sales_df = all_sales_df.with_column( "promotion_code", expr("case when promotion_code is null then 'NA' else promotion_code end"))
#     all_sales_df = all_sales_df.join(date_dim_df, ["order_dt"],join_type='inner')
#     all_sales_df = all_sales_df.join(customer_dim_df, ["customer_name","region","country"],join_type='inner')
#     all_sales_df = all_sales_df.join(payment_dim_df, ["payment_method", "payment_provider", "country", "region"],join_type='inner')
#     #all_sales_df = all_sales_df.join(product_dim_df, ["brand","model","color","Memory"],join_type='inner')
#     all_sales_df = all_sales_df.join(product_dim_df, ["mobile_key"],join_type='inner')
#     all_sales_df = all_sales_df.join(promo_code_dim_df, ["promotion_code","country", "region"],join_type='inner')
#     all_sales_df = all_sales_df.join(region_dim_df, ["country", "region"],join_type='inner')
#     all_sales_df = all_sales_df.selectExpr("sales_dwh.consumption.sales_fact_seq.nextval as order_id_pk, \
#                                            order_id as order_code,                               \
#                                            date_id_pk as date_id_fk,          \
#                                            region_id_pk as region_id_fk,            \
#                                            customer_id_pk as customer_id_fk,        \
#                                            payment_id_pk as payment_id_fk,          \
#                                            product_id_pk as product_id_fk,          \
#                                            promo_code_id_pk as promo_code_id_fk,    \
#                                            order_quantity,                          \
#                                            local_total_order_amt,                   \
#                                            local_tax_amt,                           \
#                                            exhchange_rate,                          \
#                                            us_total_order_amt,                      \
#                                            usd_tax_amt                              \
#                                            ")
#     all_sales_df.write.save_as_table("sales_dwh.consumption.sales_fact",mode="append")


# if __name__ == '__main__':
#     main()