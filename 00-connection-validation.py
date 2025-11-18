# Import libraries
from snowflake.snowpark import Session
import sys
import logging
import json

# Initiate logging at INFO level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, 
                    format='%(asctime)s; %(levelname)s; %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Snowpark Session
def get_snowpark_session() -> Session:

    # Read parameters from file
    connection_parameters = json.load(open('00-connection-validation.json'))

    # Creating Snowflake Session object
    return Session.builder.configs(connection_parameters).create()   

def main():
    # Creating Snowflake Session object
    session = get_snowpark_session()

    # Test query 1
    query1 = "SELECT current_role(), current_database(), current_schema(), current_warehouse()"
    context_df = session.sql(query1)
    context_df.show(2)

    # Test query 2
    query2 = """
        SELECT c_custkey, c_name, c_phone, c_mktsegment 
        FROM snowflake_sample_data.tpch_sf1.customer 
        LIMIT 10
    """
    customer_df = session.sql(query2)
    customer_df.show(5)

if __name__ == '__main__':
    main()  