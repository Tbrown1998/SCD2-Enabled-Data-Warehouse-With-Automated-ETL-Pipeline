# db.py
import logging
import psycopg2
from psycopg2 import sql
from config.config import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD


def connect_postgres(dbname=None):
    dbname = dbname or PG_DB
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            dbname=dbname,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT
        )
        conn.autocommit = True
        logging.info(f"=== Connected to PostgreSQL database '{dbname}' ===")
        return conn
    except psycopg2.Error as e:
        logging.error("=== Error connecting to PostgreSQL: %s ===", e)
        return None

def ensure_staging_schema(conn):
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        logging.info("=== Ensured 'staging' schema exists. ===")
    except Exception as e:
        logging.error("=== Error ensuring staging schema: %s ===", e)
    finally:
        cur.close()

def execute_create_table(conn, create_sql, table_name):
    """
    Create table from CREATE SQL. Does NOT truncate.
    Returns nothing; logs errors.
    """
    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        logging.info(f"=== Successfully created/ensured table {table_name}. ===")
    except Exception as e:
        logging.error(f"=== Error creating table {table_name}: {e} ===")
    finally:
        cur.close()

def truncate_table(conn, table_name):
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE {table_name};")
        logging.info(f"=== Table {table_name} truncated successfully. ===")
    except Exception as e:
        logging.error(f"=== Error truncating table {table_name}: {e} ===")
    finally:
        cur.close()
