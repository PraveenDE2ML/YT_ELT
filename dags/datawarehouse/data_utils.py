from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor #This is used to fetch the postgress queries as standard python dictionaries

table = "yt_api"

#function to get connection and cursor
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id = "postgres_db_yt_elt", database = "elt_db")
    conn = hook.get_conn() #establishing the connection
    cur = conn.cursor(cursor_factory = RealDictCursor) #creating a cursor with RealDictCursor to get dictionary-like output
    return conn, cur

#function to close connection and cursor
def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()

def create_schema_if_not_exists(schema_name):
    conn, cur =get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name};" # SQL query to create schema if not exists
    cur.execute(schema_sql) #execute the query
    conn.commit() #commit the changes to the database
    close_conn_cursor(conn, cur) #close the connection and cursor
    print(f"Schema '{schema_name}' ensured to exist.")

def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_Id" VARCHAR PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR NOT NULL,
                    "view_count" INT,
                    "like_count" INT,
                    "comment_count" INT,
                    "fetched_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
    else:
        table_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    "Video_Id" VARCHAR PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" TIME NOT NULL,
                    "view_count" INT,
                    "like_count" INT,
                    "comment_count" INT,
                    "processed_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
    cur.execute(table_sql) #execute the query
    conn.commit() #commit the changes to the database
    close_conn_cursor(conn, cur)

def get_video_ids(cur, schema):
    cur.execute(f""" SELECT "Video_Id" FROM {schema}.{table};""")
    ids = cur.fetchall()
    video_ids = [row['Video_Id'] for row in ids]
    return video_ids
