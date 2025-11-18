from snowflake.snowpark import Session
import os
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


def search_files(directory_to_search, extension_to_search) -> list:
    """
    Function searchs for files with needed file extention 
    in provided directory and subdirectories.

    Args:
        directory_to_search (str): Directory to search (linux format, absolute path).
        extension_to_search (str): File extention. Example: ".csv".
    
    Returns:
        list: List of tuples. Tuple consist of 
            (File name, Subfolder related to search directory, Full file path).
            Example: ('order-20200103.csv', 'source=IN/format=csv/date=2020-01-03', 
            '/tmp/somedir/source=IN/format=csv/date=2020-01-03/order-20200103.csv')

    Raises:
        ValueError: If any validation check fails.
    """

    try:
        assert len(directory_to_search) > 2
        assert directory_to_search[0] == "/"
        assert len(extension_to_search) > 0
    except ValueError:
        print('Check your path to search and/or file extention value(s)')

    files_list = []
    for root, dirs, files in os.walk(directory_to_search):
        for file in files:
            if file.endswith(extension_to_search):
                files_list.append((
                    file,
                    root.replace(directory_to_search, ""),
                    os.path.join(root, file)
                ))
    return files_list


def put_to_stage(files_list, stage_location):
    "Function gets local files and put them to Snowflake stage"

    if len(files_list) == 0:
        raise ValueError('No file found in provided list.')

    try:
        assert len(files_list) > 0
        assert len(files_list[0][0]) > 0
    except ValueError:
        print('Check the contents of the file list.')

    for index in range(len(files_list)):
        '''get_snowpark_session().file.put(
                Full file path,
                Path to put at snowflake schema staging,
                other params ...
            )
        '''
        put_result = get_snowpark_session().file.put( 
                            files_list[index][2], 
                            stage_location + "/sales/" + files_list[index][1], 
                            auto_compress=False, overwrite=True, parallel=10
                        )


def main():
    # Specify the directory path to search
    directory_to_search = '/Users/yuriidd/git/e2e-dataengineering-with-snowflake-import-files/sales-data-copy/'
    stage_location = '@SALES_DWH.RAW.RAW_INTERNAL_STG'
    

    # Looking for files
    csv_list = search_files(directory_to_search, '.csv')
    parquet_list = search_files(directory_to_search, '.parquet')
    json_list = search_files(directory_to_search, '.json')

    # Putting files to Snowflake stage 
    put_to_stage(csv_list, stage_location)
    put_to_stage(parquet_list, stage_location)
    put_to_stage(json_list, stage_location)


if __name__ == '__main__':
    main()