from datawarehouse.data_utils import *
from datawarehouse.data_tansformation import transform_data
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from airflow.decorators import task
import logging

logger = logging.getLogger(__name__)
table = "yt_api"

@task(task_id="dwh_staging_table")
def staging_table():
    schema = "staging"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()
        YT_data = load_data()
        create_schema_if_not_exists(schema)
        create_table(schema)
        logger.info(f"Schema and table created in {schema} schema.")

        table_ids = get_video_ids(cur, schema)

        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['videoId'] not in table_ids:
                    insert_rows(cur, conn, schema, row)
                else:
                    update_rows(cur, conn, schema, row)
        
        ids_in_json = {row['videoId'] for row in YT_data}

        ids_to_delete = set(table_ids) - ids_in_json #we can use set difference(minus operator) to get the ids to delete

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        logger.info(f"{schema} table updated successfully.")
    
    except Exception as e:
        logger.error(f"An error  occured during the update of {schema} table: {e}")
        raise e

    finally: #finally keyword is used to define a block of code that is guaranteed to run regardless of whether an exception occurred or was handled. 
             #it s primarily used for cleanup tasks like closing files or releasing database connections to ensure system resources are not left hanging if an error occurs.finally keyword is used to define a block of code that is guaranteed to run regardless of whether an exception occurred or was handled. It is primarily used for cleanup tasks like closing files or releasing database connections to ensure system resources are not left hanging if an error occurs.
        if conn and cur:
            close_conn_cursor(conn, cur)

@task(task_id="dwh_core_table")

def core_table():
    schema = "core"
    conn, cur = None, None

    try:
        #establish connection and cursor
        conn, cur = get_conn_cursor()
        #create schema and table if not exists
        create_schema_if_not_exists(schema)
        create_table(schema)
        logger.info(f"Schema and table created in {schema} schema.")
        #get video ids from core table(target)
        table_ids = get_video_ids(cur, schema)
        logger.info(f"Fetched {len(table_ids)} video IDs from core table.")
        #declare a set to hold current video ids from staging table(source)
        current_video_ids = set()
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall() #fetch all rows from staging table
        logger.info(f"Fetched {len(rows)} rows from staging table.")
        #loop through each row from staging table
        for row in rows:
            
            current_video_ids.add(row['Video_Id']) #add videoId to the set
             #if no ids in core table, insert all rows
            if len(table_ids) == 0:
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
                
            
            else: #if ids exist in core table
                
                transformed_row = transform_data(row) #transform the row data to match core table schema
                if transformed_row['Video_Id'] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    insert_rows(cur, conn, schema, transformed_row)
        logger.info(f"Data from staging table processed for core table update.")
        #determine ids to delete from core table
        ids_to_delete = set(table_ids) - current_video_ids #we can use set difference(minus operator) to get the ids to delete
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        logger.info(f"{schema} table updated successfully.")
    except Exception as e:
        logger.error(f"An error  occured during the update of {schema} table: {e}")
        raise e
    finally: #finally keyword is used to define a block of code that is guaranteed to run regardless of whether an exception occurred or was handled. 
             #it s primarily used for cleanup tasks like closing files or releasing database connections to ensure system resources are not left hanging if an error occurs.finally keyword is used to define a block of code that is guaranteed to run regardless of whether an exception occurred or was handled. It is primarily used for cleanup tasks like closing files or releasing database connections to ensure system resources are not left hanging if an error occurs.
        if conn and cur:
            close_conn_cursor(conn, cur)