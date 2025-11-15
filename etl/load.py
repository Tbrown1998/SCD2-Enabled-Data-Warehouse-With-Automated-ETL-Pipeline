# load.py
import logging
from psycopg2 import extras

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

def load_users_to_staging(cur, conn, df_users):
    query = """
    INSERT INTO staging.stg_users 
    (id, email, username, password, name_first, name_last, address_city, address_street, 
     address_number, address_zipcode, address_geolocation_lat, address_geolocation_long, phone)
    VALUES %s;
    """
    # map normalized DataFrame columns to insert order:
    # name_firstname, name_lastname are the normalized names produced by json_normalize with sep='_'
    # address_geolocation_lat and address_geolocation_long correspond to address.geolocation.lat/long
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

def load_carts_to_staging(cur, conn, flattened_rows):
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
