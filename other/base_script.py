# ============================================
# Dependencies
# ============================================

import requests
import pandas as pd
import psycopg2
from psycopg2 import extras
import logging

# ============================================
# Logging Setup
# ============================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s',
    handlers=[
        logging.FileHandler("etl_staging.log"),
        logging.StreamHandler()
    ]
)

# ============================================
# API Data Extraction
# ============================================

def fetch_api_data():
    """Fetch product, user, cart, and category data from fakestoreapi."""
    logging.info("=== Fetching data from fakestoreapi... ===")
    products = requests.get("https://fakestoreapi.com/products").json()
    users = requests.get("https://fakestoreapi.com/users").json()
    carts = requests.get("https://fakestoreapi.com/carts").json()
    categories = requests.get("https://fakestoreapi.com/products/categories").json()

    logging.info("=== API requests completed successfully. ===")
    return products, users, carts, categories


# ============================================
# Data Normalization
# ============================================

def normalize_data(products, users, carts, categories):
    """Normalize JSON data into flat DataFrames."""
    logging.info("=== Normalizing API JSON responses into DataFrames... ===")

    df_products = pd.json_normalize(products, sep='_')
    df_users = pd.json_normalize(users, sep='_')
    df_carts = pd.json_normalize(carts, sep='_')

    # Categories are a simple list of strings
    df_categories = pd.DataFrame(categories, columns=['category_name'])

    logging.info("=== Data normalization complete. ===")
    return df_products, df_users, df_carts, df_categories


# ============================================
# PostgreSQL Connection Setup
# ============================================

def connect_postgres(staging_db_name='staging_ecommerce'):
    """Establish connection to PostgreSQL and ensure 'staging' schema exists."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            dbname=staging_db_name,
            password="",
            user="postgres",
            port="5432"
        )
        conn.autocommit = True
        logging.info(f"=== Connected to PostgreSQL database '{staging_db_name}' ===")

        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        logging.info("=== Ensured 'staging' schema exists. ===")

        return conn, cur

    except psycopg2.Error as e:
        logging.error("=== Error connecting to PostgreSQL: %s ===", e)
        return None, None


# ============================================
# Generic Table Creator
# ============================================

def execute_table_creation(conn, create_sql, table_name):
    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        logging.info(f"=== Successfully created table {table_name}. ===")
    except Exception as e:
        logging.error(f"=== Error creating table {table_name}: {e} ===")

    try:
        cur.execute(f"TRUNCATE TABLE {table_name};")
        logging.info(f"=== Table {table_name} truncated successfully. ===")
    except Exception as e:
        logging.error(f"=== Error truncating table {table_name}: {e} ===")
    return cur


# ============================================
# Products Table Creation + Load
# ============================================

def create_products_staging_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS staging.stg_products (
        id TEXT,
        title TEXT,
        price FLOAT,
        description TEXT,
        category TEXT,
        image TEXT,
        rating_rate FLOAT,
        rating_count INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    return execute_table_creation(conn, sql, "staging.stg_products")


def load_products_to_staging(cur, conn, df_products):
    query = """
    INSERT INTO staging.stg_products 
    (id, title, price, description, category, image, rating_rate, rating_count)
    VALUES %s;
    """
    records = list(
        df_products[['id', 'title', 'price', 'description', 'category', 'image', 'rating_rate', 'rating_count']]
        .itertuples(index=False, name=None)
    )
    try:
        extras.execute_values(cur, query, records)
        logging.info("=== Inserted products data successfully. ===")
    except Exception as e:
        logging.error("=== Error inserting products data: %s ===", e)
        conn.rollback()


# ============================================
# Users Table Creation + Load
# ============================================

def create_users_staging_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS staging.stg_users (
        id INT,
        email TEXT,
        username TEXT,
        password TEXT,
        name_first TEXT,
        name_first TEXT,
        address_city TEXT,
        address_street TEXT,
        address_number INT,
        address_zipcode TEXT,
        address_geolocation_lat FLOAT,
        address_geolocation_long FLOAT,
        phone TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    return execute_table_creation(conn, sql, "staging.stg_users")


def load_users_to_staging(cur, conn, df_users):
    query = """
    INSERT INTO staging.stg_users 
    (id, email, username, password, name_first, name_last, address_city, address_street, 
     address_number, address_zipcode, address_geolocation_lat, address_geolocation_long, phone)
    VALUES %s;
    """

    # Correct column references based on normalized names
    records = list(
        df_users[['id', 'email', 'username', 'password', 'name_firstname', 'name_lastname',
                  'address_city', 'address_street', 'address_number', 'address_zipcode',
                  'address_geolocation_lat', 'address_geolocation_long', 'phone']]
        .itertuples(index=False, name=None)
    )
    try:
        extras.execute_values(cur, query, records)
        logging.info("=== Inserted users data successfully. ===")
    except Exception as e:
        logging.error("=== Error inserting users data: %s ===", e)
        conn.rollback()


# ============================================
# Carts Table Creation + Load
# ============================================

def create_carts_staging_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS staging.stg_carts (
        cart_id INT,
        user_id INT,
        date TIMESTAMP,
        product_id INT,
        quantity INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    return execute_table_creation(conn, sql, "staging.stg_carts")


def load_carts_to_staging(cur, conn, df_carts):
    logging.info("=== Flattening and preparing carts data for insertion... ===")

    # Flatten nested products list into rows
    flattened_rows = []
    for _, row in df_carts.iterrows():
        cart_id = row['id']
        user_id = row['userId']
        date = row['date']
        for product in row['products']:
            flattened_rows.append((
                cart_id,
                user_id,
                date,
                product.get('productId'),
                product.get('quantity')
            ))

    query = """
    INSERT INTO staging.stg_carts (cart_id, user_id, date, product_id, quantity)
    VALUES %s;
    """

    try:
        extras.execute_values(cur, query, flattened_rows)
        logging.info("=== Inserted flattened carts data successfully. ===")
    except Exception as e:
        logging.error("=== Error inserting carts data: %s ===", e)
        conn.rollback()

# ============================================
# Categories Table Creation + Load
# ============================================

def create_categories_staging_table(conn):
    sql = """
    CREATE TABLE IF NOT EXISTS staging.stg_categories (
        category_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    return execute_table_creation(conn, sql, "staging.stg_categories")


def load_categories_to_staging(cur, conn, df_categories):
    query = """
    INSERT INTO staging.stg_categories (category_name)
    VALUES %s;
    """
    records = [(cat,) for cat in df_categories['category_name']]
    try:
        extras.execute_values(cur, query, records)
        logging.info("=== Inserted categories data successfully. ===")
    except Exception as e:
        logging.error("=== Error inserting categories data: %s ===", e)
        conn.rollback()


# ============================================
# Main Orchestration
# ============================================

def main():
    logging.info("=== Starting ETL pipeline for staging_ecommerce... ===")

    products, users, carts, categories = fetch_api_data()
    df_products, df_users, df_carts, df_categories = normalize_data(products, users, carts, categories)

    conn, cur = connect_postgres()
    if not conn:
        logging.critical("=== Database connection failed. Terminating ETL process. ===")
        return

    try:
        create_products_staging_table(conn)
        create_users_staging_table(conn)
        create_carts_staging_table(conn)
        create_categories_staging_table(conn)

        load_products_to_staging(cur, conn, df_products)
        load_users_to_staging(cur, conn, df_users)
        load_carts_to_staging(cur, conn, df_carts)
        load_categories_to_staging(cur, conn, df_categories)

        logging.info("=== ETL process completed successfully! ===")

    except Exception as e:
        logging.error("=== ETL process encountered an error: %s ===", e)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("=== Connection closed. ===")


# ============================================
# Entry Point
# ============================================

if __name__ == "__main__":
    main()
