import pandas as pd

def extract_keys(obj, parent_key='', results=None):
    """
    Recursively extract all unique key paths from nested dicts, lists, or DataFrames
    and record the datatype of each final value.
    """
    if results is None:
        results = {}

    # Handle DataFrame
    if isinstance(obj, pd.DataFrame):
        for col in obj.columns:
            col_key = f"{parent_key}.{col}" if parent_key else col
            # Apply recursion to the first non-null cell (if nested)
            sample_val = obj[col].dropna().iloc[0] if not obj[col].dropna().empty else None
            extract_keys(sample_val, col_key, results)

    elif isinstance(obj, dict):
        for k, v in obj.items():
            full_key = f"{parent_key}.{k}" if parent_key else k
            extract_keys(v, full_key, results)

    elif isinstance(obj, list):
        for item in obj:
            extract_keys(item, parent_key, results)

    else:
        # Base case
        results[parent_key] = type(obj).__name__

    return results
