# sql_defs.py

def create_products_sql():
    return """
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

def create_users_sql():
    # fixed duplicate column (name_first repeated) -> name_first, name_last
    return """
    CREATE TABLE IF NOT EXISTS staging.stg_users (
        id INT,
        email TEXT,
        username TEXT,
        password TEXT,
        name_first TEXT,
        name_last TEXT,
        address_city TEXT,
        address_street TEXT,
        address_number INT,
        address_zipcode TEXT,
        address_geolocation_lat TEXT,
        address_geolocation_long TEXT,
        phone TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

def create_carts_sql():
    return """
    CREATE TABLE IF NOT EXISTS staging.stg_carts (
        cart_id INT,
        user_id INT,
        date TIMESTAMP,
        product_id INT,
        quantity INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

def create_categories_sql():
    return """
    CREATE TABLE IF NOT EXISTS staging.stg_categories (
        category_name TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
