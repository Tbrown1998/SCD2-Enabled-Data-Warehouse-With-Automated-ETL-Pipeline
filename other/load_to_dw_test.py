#!/usr/bin/env python3
"""
ETL: Load from staging -> Data Warehouse (DW)
- Uses SCD Type 2 for customers
- Uses Type 1 upsert for products & stores
- Appends facts (with dedup protection)
- Logs using === style
"""

import os
import hashlib
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date, timedelta

# ---------------------------
# CONFIGURATION - change these for your environment
# ---------------------------
DB_CONFIG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": os.getenv("PGPORT", "5432"),
    "dbname": os.getenv("PGDATABASE", "your_db"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "yourpassword"),
}

STAGING_SCHEMA = "staging"
DW_SCHEMA = "dw"

BATCH_SIZE = 1000

# ---------------------------
# Utilities
# ---------------------------
def log(msg):
    print(f"=== {msg} ===")

def row_hash(row: dict, keys: list):
    """
    compute md5 hash of concatenated values for change detection.
    Keys order matters; ensure deterministic inputs (cast to str).
    """
    concat = "|".join("" if row.get(k) is None else str(row.get(k)) for k in keys)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()

# ---------------------------
# DW Creation / DDL
# ---------------------------
CREATE_TABLES_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA};

-- surrogate key dims
CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_date (
    date_id DATE PRIMARY KEY,
    day SMALLINT,
    month SMALLINT,
    year SMALLINT,
    quarter SMALLINT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_product (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id VARCHAR(100) UNIQUE,
    title TEXT,
    category TEXT,
    price NUMERIC,
    data_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_store (
    store_sk BIGSERIAL PRIMARY KEY,
    store_id VARCHAR(100) UNIQUE,
    store_name TEXT,
    location TEXT,
    country TEXT,
    data_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

-- SCD Type 2: customer dimension
CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_id VARCHAR(100),
    full_name TEXT,
    email TEXT,
    phone TEXT,
    country TEXT,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    data_hash VARCHAR(64),
    UNIQUE(customer_id, start_date)  -- allow historical rows
);

-- Facts
CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.fact_sales (
    sale_sk BIGSERIAL PRIMARY KEY,
    sale_id VARCHAR(100) UNIQUE,  -- unique source id for dedupe
    product_sk BIGINT REFERENCES {DW_SCHEMA}.dim_product(product_sk),
    customer_sk BIGINT REFERENCES {DW_SCHEMA}.dim_customer(customer_sk),
    store_sk BIGINT REFERENCES {DW_SCHEMA}.dim_store(store_sk),
    date_id DATE REFERENCES {DW_SCHEMA}.dim_date(date_id),
    quantity INT,
    price NUMERIC,
    total_amount NUMERIC,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {DW_SCHEMA}.fact_cart (
    cart_sk BIGSERIAL PRIMARY KEY,
    cart_id VARCHAR(100) UNIQUE,
    customer_sk BIGINT REFERENCES {DW_SCHEMA}.dim_customer(customer_sk),
    created_at TIMESTAMP,
    total_items INT,
    total_value NUMERIC
);
"""

# ---------------------------
# Core ETL functions
# ---------------------------
def create_dw_schema_and_tables(conn):
    log("creating DW schema and tables")
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLES_SQL)
    conn.commit()
    log("DW schema/tables ready")

def load_dim_date(conn, start_date='2020-01-01', end_date=None):
    """
    Populate dim_date for a date range.
    If end_date is None -> today + 365 days.
    """
    if end_date is None:
        end_date = (date.today() + timedelta(days=365)).isoformat()
    log(f"loading dim_date from {start_date} to {end_date}")

    d0 = date.fromisoformat(start_date)
    d1 = date.fromisoformat(end_date)
    rows = []
    cur_date = d0
    while cur_date <= d1:
        rows.append((
            cur_date,
            cur_date.day,
            cur_date.month,
            cur_date.year,
            (cur_date.month - 1) // 3 + 1,
            cur_date.weekday() >= 5
        ))
        cur_date += timedelta(days=1)

    with conn.cursor() as cur:
        execute_values(cur,
            f"""
            INSERT INTO {DW_SCHEMA}.dim_date (date_id, day, month, year, quarter, is_weekend)
            VALUES %s
            ON CONFLICT (date_id) DO NOTHING
            """,
            rows
        )
    conn.commit()
    log("dim_date loaded")

def upsert_dim_product(conn):
    """
    Type 1 upsert for products: overwrite attributes if anything changed.
    Uses data_hash to detect changes and minimize writes.
    """
    log("upserting dim_product (Type1)")
    staging_table = f"{STAGING_SCHEMA}.stg_products"
    # keys used for hash detection
    hash_keys = ["title", "category", "price"]

    with conn.cursor() as cur:
        # fetch staging in batches
        cur.execute(f"SELECT id, title, category, price FROM {staging_table}")
        rows = cur.fetchall()
        if not rows:
            log("no rows in staging stg_products")
            return

        for (id_, title, category, price) in rows:
            src = {"title": title, "category": category, "price": price}
            h = row_hash(src, hash_keys)
            # upsert: if exists and hash differs -> update, else insert
            cur.execute(f"""
                INSERT INTO {DW_SCHEMA}.dim_product (product_id, title, category, price, data_hash, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (product_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    data_hash = EXCLUDED.data_hash,
                    updated_at = now()
                WHERE {DW_SCHEMA}.dim_product.data_hash IS DISTINCT FROM EXCLUDED.data_hash
            """, (str(id_), title, category, price, h))
    conn.commit()
    log("dim_product upsert complete")

def upsert_dim_store(conn):
    """
    Simple Type 1 upsert for stores (if you have staging.stg_stores).
    """
    log("upserting dim_store (Type1)")
    staging_table = f"{STAGING_SCHEMA}.stg_stores"
    hash_keys = ["store_name", "location", "country"]

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT storekey, store_name, location, country
            FROM {staging_table}
        """)
        rows = cur.fetchall()
        if not rows:
            log("no rows in staging stg_stores")
            return

        for (store_id, store_name, location, country) in rows:
            src = {"store_name": store_name, "location": location, "country": country}
            h = row_hash(src, hash_keys)
            cur.execute(f"""
                INSERT INTO {DW_SCHEMA}.dim_store (store_id, store_name, location, country, data_hash, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, now(), now())
                ON CONFLICT (store_id)
                DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    location = EXCLUDED.location,
                    country = EXCLUDED.country,
                    data_hash = EXCLUDED.data_hash,
                    updated_at = now()
                WHERE {DW_SCHEMA}.dim_store.data_hash IS DISTINCT FROM EXCLUDED.data_hash
            """, (str(store_id), store_name, location, country, h))
    conn.commit()
    log("dim_store upsert complete")

def scd2_upsert_customers(conn):
    """
    SCD Type 2 for customers:
    - If customer doesn't exist: insert as current
    - If exists and hash changed: set current row end_date + insert new current version
    - If exists and hash same: do nothing
    """
    log("scd2 upsert for dim_customer")
    staging_table = f"{STAGING_SCHEMA}.stg_customers"
    hash_keys = ["full_name", "email", "phone", "country"]

    with conn.cursor() as cur:
        cur.execute(f"SELECT id, full_name, email, phone, country, created_at FROM {staging_table}")
        rows = cur.fetchall()
        if not rows:
            log("no rows in staging stg_customers")
            return

        for (cust_id, full_name, email, phone, country, created_at) in rows:
            src = {"full_name": full_name, "email": email, "phone": phone, "country": country}
            h = row_hash(src, hash_keys)

            # check current record
            cur.execute(f"""
                SELECT customer_sk, data_hash
                FROM {DW_SCHEMA}.dim_customer
                WHERE customer_id = %s AND is_current = TRUE
                LIMIT 1
            """, (str(cust_id),))
            current = cur.fetchone()

            if current is None:
                # insert new current row
                cur.execute(f"""
                    INSERT INTO {DW_SCHEMA}.dim_customer
                    (customer_id, full_name, email, phone, country, start_date, is_current, data_hash)
                    VALUES (%s, %s, %s, %s, %s, now(), TRUE, %s)
                """, (str(cust_id), full_name, email, phone, country, h))
                log(f"Inserted new customer {cust_id}")
            else:
                customer_sk, existing_hash = current
                if existing_hash != h:
                    # close current row
                    cur.execute(f"""
                        UPDATE {DW_SCHEMA}.dim_customer
                        SET end_date = now(), is_current = FALSE
                        WHERE customer_sk = %s
                    """, (customer_sk,))
                    # insert new current row with updated values
                    cur.execute(f"""
                        INSERT INTO {DW_SCHEMA}.dim_customer
                        (customer_id, full_name, email, phone, country, start_date, is_current, data_hash)
                        VALUES (%s, %s, %s, %s, %s, now(), TRUE, %s)
                    """, (str(cust_id), full_name, email, phone, country, h))
                    log(f"Customer {cust_id} changed -> closed old row and inserted new")
                else:
                    # no change
                    log(f"No change for customer {cust_id}")
    conn.commit()
    log("scd2 customer processing complete")

def append_fact_sales(conn):
    """
    Append-only load for fact_sales with dedup protection using sale_id.
    Uses staging.stg_sales with columns: id, product_id, user_id, storekey, date (YYYY-MM-DD), quantity, price
    """
    log("appending fact_sales")
    staging_table = f"{STAGING_SCHEMA}.stg_sales"

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, product_id, user_id, storekey, date, quantity, price
            FROM {staging_table}
        """)
        rows = cur.fetchall()
        if not rows:
            log("no rows in staging stg_sales")
            return

        # For each row, resolve foreign keys to SKs (product_sk, customer_sk (current), store_sk, date_id)
        inserts = []
        for (sale_id, product_id, user_id, storekey, date_txt, quantity, price) in rows:
            # check if sale already exists
            cur.execute(f"SELECT 1 FROM {DW_SCHEMA}.fact_sales WHERE sale_id = %s", (str(sale_id),))
            if cur.fetchone():
                log(f"sale {sale_id} already exists -> skipping")
                continue

            # resolve product_sk
            cur.execute(f"SELECT product_sk FROM {DW_SCHEMA}.dim_product WHERE product_id = %s LIMIT 1", (str(product_id),))
            prod_row = cur.fetchone()
            product_sk = prod_row[0] if prod_row else None

            # resolve customer_sk (current)
            cur.execute(f"SELECT customer_sk FROM {DW_SCHEMA}.dim_customer WHERE customer_id = %s AND is_current = TRUE LIMIT 1", (str(user_id),))
            cust_row = cur.fetchone()
            customer_sk = cust_row[0] if cust_row else None

            # resolve store_sk
            cur.execute(f"SELECT store_sk FROM {DW_SCHEMA}.dim_store WHERE store_id = %s LIMIT 1", (str(storekey),))
            store_row = cur.fetchone()
            store_sk = store_row[0] if store_row else None

            # date
            try:
                date_id = date.fromisoformat(str(date_txt))
            except Exception:
                date_id = None

            total_amount = (price or 0) * (quantity or 0)
            inserts.append((str(sale_id), product_sk, customer_sk, store_sk, date_id, quantity, price, total_amount))

        if inserts:
            execute_values(cur, f"""
                INSERT INTO {DW_SCHEMA}.fact_sales
                (sale_id, product_sk, customer_sk, store_sk, date_id, quantity, price, total_amount)
                VALUES %s
            """, inserts)
            conn.commit()
            log(f"Inserted {len(inserts)} rows into fact_sales")
        else:
            log("No new fact_sales to insert")

def append_fact_cart(conn):
    """
    Append-only load for carts with dedupe on cart_id
    staging.stg_carts expected columns: id, user_id, created_at, total_items, total_value
    """
    log("appending fact_cart")
    staging_table = f"{STAGING_SCHEMA}.stg_carts"

    with conn.cursor() as cur:
        cur.execute(f"SELECT id, user_id, created_at, total_items, total_value FROM {staging_table}")
        rows = cur.fetchall()
        if not rows:
            log("no rows in staging stg_carts")
            return

        inserts = []
        for (cart_id, user_id, created_at, total_items, total_value) in rows:
            # skip existing cart
            cur.execute(f"SELECT 1 FROM {DW_SCHEMA}.fact_cart WHERE cart_id = %s", (str(cart_id),))
            if cur.fetchone():
                log(f"cart {cart_id} already exists -> skipping")
                continue
            # resolve customer_sk
            cur.execute(f"SELECT customer_sk FROM {DW_SCHEMA}.dim_customer WHERE customer_id = %s AND is_current = TRUE LIMIT 1", (str(user_id),))
            cust_row = cur.fetchone()
            customer_sk = cust_row[0] if cust_row else None

            inserts.append((str(cart_id), customer_sk, created_at, total_items, total_value))

        if inserts:
            execute_values(cur, f"""
                INSERT INTO {DW_SCHEMA}.fact_cart
                (cart_id, customer_sk, created_at, total_items, total_value)
                VALUES %s
            """, inserts)
            conn.commit()
            log(f"Inserted {len(inserts)} rows into fact_cart")
        else:
            log("No new fact_cart to insert")

# ---------------------------
# Main Orchestration
# ---------------------------
def main():
    log("starting staging -> DW load")
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        create_dw_schema_and_tables(conn)
        load_dim_date(conn, start_date="2019-01-01")     # adjust as needed

        # Upsert dimensions
        upsert_dim_product(conn)
        upsert_dim_store(conn)
        scd2_upsert_customers(conn)

        # Append facts
        append_fact_sales(conn)
        append_fact_cart(conn)

        log("ETL load complete")
    except Exception as e:
        log(f"ETL failed: {e}")
        raise
    finally:
        conn.close()
        log("connection closed")

if __name__ == "__main__":
    main()
