# extract.py
import logging
import requests

def fetch_api_data():
    """Fetch product, user, cart, and category data from fakestoreapi."""
    logging.info("=== Fetching data from fakestoreapi... ===")
    products = requests.get("https://fakestoreapi.com/products").json()
    users = requests.get("https://fakestoreapi.com/users").json()
    carts = requests.get("https://fakestoreapi.com/carts").json()
    categories = requests.get("https://fakestoreapi.com/products/categories").json()

    logging.info("=== API requests completed successfully. ===")
    return products, users, carts, categories
