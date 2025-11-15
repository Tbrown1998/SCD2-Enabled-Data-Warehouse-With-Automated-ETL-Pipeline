# main.py
from config.logging_config import configure_logging
from db.db import truncate_table
from db.db import connect_postgres, ensure_staging_schema, execute_create_table
from sql.sql_defs import (
    create_products_sql, 
    create_users_sql, 
    create_carts_sql, 
    create_categories_sql
)

from etl.extract import fetch_api_data
from etl.transform import normalize_data, flatten_carts_df
from etl.load import (
    load_products_to_staging,
    load_users_to_staging,
    load_carts_to_staging,
    load_categories_to_staging
)
import logging
from db.run_procedures import run_procedure_sequence

def main():
    configure_logging()
    logging.info("=== Starting ETL pipeline for staging_ecommerce... ===")

    products, users, carts, categories = fetch_api_data()
    df_products, df_users, df_carts, df_categories = normalize_data(products, users, carts, categories)
    flattened_carts = flatten_carts_df(df_carts)

    conn = connect_postgres()
    if not conn:
        logging.critical("=== Database connection failed. Terminating ETL process. ===")
        return

    try:
        ensure_staging_schema(conn)

        execute_create_table(conn, create_products_sql(), "staging.stg_products")
        execute_create_table(conn, create_users_sql(), "staging.stg_users")
        execute_create_table(conn, create_carts_sql(), "staging.stg_carts")
        execute_create_table(conn, create_categories_sql(), "staging.stg_categories")

        truncate_table(conn, "staging.stg_products")
        truncate_table(conn, "staging.stg_users")
        truncate_table(conn, "staging.stg_carts")
        truncate_table(conn, "staging.stg_categories")

        cur = conn.cursor()

        load_products_to_staging(cur, conn, df_products)
        load_users_to_staging(cur, conn, df_users)
        load_carts_to_staging(cur, conn, flattened_carts)
        load_categories_to_staging(cur, conn, df_categories)


        logging.info("=== Executing stored procedures ===")
        PROCEDURE_SEQUENCE = [
            "dw.usp_upsert_dim_category",
            "dw.usp_scd2_customer",
            "dw.usp_upsert_dim_product",
            "dw.usp_load_fact_cart",
            "dw.usp_load_fact_cart_item"
        ]
        run_procedure_sequence(conn, PROCEDURE_SEQUENCE)

        logging.info("=== Stored Procedures Run Completed Successfully! ===")

        logging.info("=== Full Pipeline Completed Successfully! ===")

    except Exception as e:
        logging.error("=== Pipeline encountered an error: %s ===", e)

    finally:
        try:
            cur.close()
        except:
            pass
        conn.close()
        logging.info("=== Connection closed. ===")


if __name__ == "__main__":
    main()
