# transform.py
import logging
import pandas as pd

def normalize_data(products, users, carts, categories):
    """Normalize JSON data into flat DataFrames."""
    logging.info("=== Normalizing API JSON responses into DataFrames... ===")

    df_products = pd.json_normalize(products, sep='_')
    df_users = pd.json_normalize(users, sep='_')
    df_carts = pd.json_normalize(carts, sep='_')

    df_categories = pd.DataFrame(categories, columns=['category_name'])

    logging.info("=== Data normalization complete. ===")
    return df_products, df_users, df_carts, df_categories

def flatten_carts_df(df_carts):
    """
    Produces list-of-tuples suitable for execute_values insertion:
    (cart_id, user_id, date, product_id, quantity)
    """
    logging.info("=== Flattening carts DataFrame... ===")
    flattened_rows = []
    for _, row in df_carts.iterrows():
        cart_id = row.get('id')
        user_id = row.get('userId')
        date = row.get('date')
        # 'products' column present as list of dicts in normalized result; fallback to raw if needed
        products = row.get('products', [])
        # If products were expanded by json_normalize it could be 'products' as a list-string; keep original behavior
        if isinstance(products, list):
            for product in products:
                flattened_rows.append((
                    cart_id,
                    user_id,
                    date,
                    product.get('productId'),
                    product.get('quantity')
                ))
        else:
            # In case json_normalize changed structure, try fallback:
            logging.warning("=== Unexpected products structure in carts row; skipping or trying to parse. ===")
    logging.info("=== Flattening complete. Records: %d ===", len(flattened_rows))
    return flattened_rows
