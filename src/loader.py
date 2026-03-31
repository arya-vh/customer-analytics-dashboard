import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)


def load_customers(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        logger.error(f"Customers file not found: {filepath}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(filepath)

        # Strip whitespace from column names (handles accidental spaces in headers)
        df.columns = df.columns.str.strip()

        required_columns = {"customer_id", "customer_name", "email", "status", "signup_date"}
        missing = required_columns - set(df.columns)
        if missing:
            logger.error(f"Customers file missing columns: {missing}")
            return pd.DataFrame()

        # Normalize text fields
        df["status"] = df["status"].str.strip().str.upper()
        df["customer_id"] = df["customer_id"].astype(str).str.strip()

        logger.info(f"Loaded {len(df)} customer records from {filepath}")
        return df

    except Exception as e:
        logger.error(f"Failed to load customers file: {e}")
        return pd.DataFrame()


def load_orders(filepath: str) -> pd.DataFrame:
    if not os.path.exists(filepath):
        logger.error(f"Orders file not found: {filepath}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(filepath)

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        required_columns = {"order_id", "customer_id", "order_date", "order_amount", "order_status"}
        missing = required_columns - set(df.columns)
        if missing:
            logger.error(f"Orders file missing columns: {missing}")
            return pd.DataFrame()

        # Normalize fields
        df["order_status"] = df["order_status"].str.strip().str.upper()
        df["customer_id"] = df["customer_id"].astype(str).str.strip()
        df["order_id"] = df["order_id"].astype(str).str.strip()

        logger.info(f"Loaded {len(df)} order records from {filepath}")
        return df

    except Exception as e:
        logger.error(f"Failed to load orders file: {e}")
        return pd.DataFrame()