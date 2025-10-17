import pandas as pd
from sqlalchemy import Date, create_engine, String, Numeric
from textblob import TextBlob
import os
import logging

"""
# ──────────────────────────────────────────────────────────────────────────────
# 📋 Missing Value Reference Guide
# ──────────────────────────────────────────────────────────────────────────────
# Looks like | Actually is                  | Type in Python | Pandas shows as                | Treated as Missing? | Should become
# -----------|------------------------------|----------------|-------------------------------|---------------------|---------------
# ""         | empty string (length = 0)    | str            | ""                            | ❌ No              | <NA>
# " "        | spaces / blank string (>0)   | str            | looks blank, but not empty    | ❌ No              | <NA>
# "nan"      | text that says "nan"         | str            | 'nan'                         | ❌ No              | <NA>
# "null"     | text that says "null"        | str            | 'null'                        | ❌ No              | <NA>
# null       | JSON/SQL null → Python None  | NoneType       | NaN (when loaded in pandas)   | ✅ Yes             | <NA>
# None       | Python None / null           | NoneType       | NaN (when loaded in pandas)   | ✅ Yes             | <NA>
# np.nan     | numeric null (NumPy NaN)     | float          | NaN                           | ✅ Yes             | <NA>
# pd.NA      | pandas null / missing value  | NAType         | <NA>                          | ✅ Yes             | <NA>
# ──────────────────────────────────────────────────────────────────────────────

"""


# =====================
# PostgreSQL Connection
# =====================
DB_USER = 'postgres'
DB_PASSWORD = 'your_password'
DB_HOST = 'your_host'
DB_PORT = 'port'
DB_NAME = 'retail_analytics'

engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


# =====================
# File Paths
# =====================
table_files = {
    'customers': 'I:\\DATA ANALYTICS PROJECTS\\retail_analytics\\raw data\\customers.csv',
    'orders': 'I:\\DATA ANALYTICS PROJECTS\\retail_analytics\\raw data\\orders.csv',
    'products': 'I:\\DATA ANALYTICS PROJECTS\\retail_analytics\\raw data\\products.csv',
    'order_items': 'I:\\DATA ANALYTICS PROJECTS\\retail_analytics\\raw data\\order_items.csv',
    'reviews': 'I:\\DATA ANALYTICS PROJECTS\\retail_analytics\\raw data\\reviews.csv'
}


# =====================
# Generic Cleaning Function
# =====================
    
"""
    Generic cleaning:
    -----------------
    1. Strip strings
    2. Replace empty/blank/nan strings with NA
    3. Convert date columns
    4. Fill numeric nulls with 0
    5. Normalize ID columns (always strings)

    pd.read_csv(), it’s always interpreted as a string/object.Excel’s formatting doesn’t carry over in CSV.
    CSV has no type information — every cell is just text/numbers.
    pd.read_csv() guesses types. If a column has mixed formats or some blanks, it often defaults to object.
    Excel formatting (Short Date, Long Date) only affects display in Excel, not the raw CSV data.

    Notes on ID columns:
    - Forces all IDs to Python strings (object dtype in pandas)
    - Strips spaces, handles any numeric-looking IDs (e.g., 1 → '1')
    - Cleans inconsistencies in raw CSV before loading
    - Even if dtype_mapping says String for PostgreSQL, pandas might infer int64 → SQLAlchemy maps it to BIGINT
    - Leading/trailing spaces or blank cells can cause foreign key mismatches or errors

    Notes on date columns:
    - CSVs often store dates as strings, even if Excel shows them as Short/Long Date
    - `pd.to_datetime` converts string/object columns to pandas datetime
    - `errors='coerce'` ensures invalid formats become NaT instead of crashing
    - `dayfirst=True/False` depends on your date format (DD/MM/YYYY vs MM/DD/YYYY)


    dt.date converts pandas datetime to Python date objects → SQLAlchemy maps this to 
    DATE in PostgreSQL. No time component, only YYYY-MM-DD
    
      Important:
            - Forces all IDs to Python strings (object dtype in pandas)
            - Strips spaces, handles numeric-looking IDs (1 → '1')
            - Prevents foreign key mismatches in PostgreSQL
    """

def clean_dataframe(df, table_name=None):
    # -----------------
    # Step 1: Basic column-wise cleaning
    # -----------------
    for col in df.columns:
        if df[col].dtype == 'object':
            # Strip spaces and replace blank strings with pd.NA
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'': pd.NA, ' ': pd.NA, 'nan': pd.NA, 'NaN': pd.NA})
        elif pd.api.types.is_numeric_dtype(df[col]):
            # Fill numeric nulls with 0
            df[col] = df[col].fillna(0)

    # -----------------
    # Step 2: Table-specific date conversion
    # -----------------
    table_date_cols = {
        'customers': ['signup_date', 'dob'],
        'orders': ['order_date'],
        'reviews': ['review_date'],
        # 'products': []  # no date columns
    }
    
    date_cols = table_date_cols.get(table_name, [])
    for col in date_cols:
        if col in df.columns:
            # Strip again and replace empty strings
            df[col] = df[col].astype(str).str.strip().replace({'': pd.NA})
            # Convert to datetime and then take only date
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.date
            """ 
            dt.date converts pandas datetime to Python date objects → SQLAlchemy maps this to 
            DATE in PostgreSQL. No time component, only YYYY-MM-DD
            """

    # -----------------
    # Step 3: Normalize all ID columns (force to string)
    # -----------------
    id_columns = ['order_id', 'customer_id', 'order_item_id', 'product_id']
    for col in id_columns:
        if col in df.columns:
            # Convert numeric-looking IDs to string, strip spaces
            df[col] = df[col].astype(str).str.strip()
            """
            Important:
            - Forces all IDs to Python strings (object dtype in pandas)
            - Strips spaces, handles numeric-looking IDs (1 → '1')
            - Prevents foreign key mismatches in PostgreSQL
            """

    print(df.dtypes)  # ✅ Debug: shows final pandas dtypes
    return df




# =====================
# Table-specific Cleaning Rules
# =====================
def clean_orders(df):
   
 # Only mark as 'Shipped' if payment_method is 'COD' AND order_status is NaN
    mask_shipped = (df['payment_method'] == 'COD') & df['order_status'].isna()

# Only mark as 'Pending' if order_status is NaN but not shipped
    mask_pending = df['order_status'].isna() & ~mask_shipped

    df.loc[mask_shipped, 'order_status'] = 'Shipped'
    df.loc[mask_pending, 'order_status'] = 'Pending'

# Fix payment_method if missing
    df['payment_method'] = df['payment_method'].fillna('Unknown')
    df.loc[df['order_status'] == 'Shipped', 'payment_method'] = 'COD'

    return df


def clean_reviews(df):
    """
    Review-specific cleaning:
    - Convert rating to numeric
    - Missing ratings remain NaN (ignored in mean calculations)
    - Fill empty review_text with ''
    """
    if 'rating' in df.columns:
        # Replace empty, space, or literal "nan" with NA first
        df['rating'] = df['rating'].replace(['', ' ', 'nan', 'NaN', 'N/A', 'null'], pd.NA)
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')  # Convert to numeric safely

    if 'review_text' in df.columns:
        df['review_text'] = df['review_text'].fillna('')

    return df


def clean_customers(df):
    """
    Customer-specific cleaning:
    - Normalize gender and email
    """
    if 'gender' in df.columns:
        df['gender'] = df['gender'].replace({'': pd.NA, 'Unknown': pd.NA})
    if 'email' in df.columns:
        df['email'] = df['email'].str.lower()
    return df



# =====================

# Define dtype mappings for each table keep the schema consistent so when we join in sql we do not
# have to cast
# Data Type Mappings for SQLAlchemy
"""
What dtype_mapping does?
Tells SQLAlchemy/to_sql() what PostgreSQL type to use when creating the table
Doesn’t automatically convert pandas data — if pandas has int/float/NaN in that column, 
it will try to cast and may cause type mismatches
"""

# Example: all IDs as VARCHAR(50)
dtype_mapping = {
    'customers': {'customer_id': String(50) },
    'orders': {'order_id': String(50), 'customer_id': String(50)},
    'products': {'product_id': String(50)},
    'order_items': {
        'order_item_id': String(50),
        'order_id': String(50),
        'product_id': String(50)
    },
    'reviews': {
        'review_id': String(50),
        'order_id': String(50),
        'customer_id': String(50),
        'product_id': String(50)
    }
}

# =====================
# ETL Loop
# =====================
for table_name, file_path in table_files.items():
    try:
        logging.info(f"Starting ETL for table: {table_name}")
        print(f"Starting ETL for table: {table_name}")

        # Extract
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} rows from {file_path}")
        print(f"Loaded {len(df)} rows from {file_path}")
        # Generic clean
        df = clean_dataframe(df, table_name)
        df = clean_dataframe(df)

        # Table-specific cleaning
        if table_name == 'orders':
            df = clean_orders(df)
        elif table_name == 'reviews':
            df = clean_reviews(df)
            # df = compute_sentiment(df)
        elif table_name == 'customers':
            df = clean_customers(df)

        # =====================
        # Load with dtype enforcement
        # =====================
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',  # or 'append' if you want to keep manual schema
            index=False,
            dtype=dtype_mapping.get(table_name)  # safely returns None if no mapping 
        )

        logging.info(f"Inserted {len(df)} rows into {table_name} table in PostgreSQL")
        print(f"Inserted {len(df)} rows into {table_name} table in PostgreSQL")

    except Exception as e:
        logging.error(f"Error in ETL for {table_name}: {e}")
        print(f"Error in ETL for {table_name}: {e}")