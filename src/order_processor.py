import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Only process orders from this period
TARGET_YEAR = 2024
TARGET_MONTH = 5  # May


def parse_order_dates(orders_df: pd.DataFrame) -> pd.DataFrame:
    df = orders_df.copy()

    # coerce=True turns bad dates into NaT instead of raising an error
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", format="mixed")

    invalid_dates = df[df["order_date"].isna()]
    if not invalid_dates.empty:
        logger.warning(
            f"Skipping {len(invalid_dates)} order(s) with invalid/missing dates: "
            f"{invalid_dates['order_id'].tolist()}"
        )

    df = df.dropna(subset=["order_date"])
    return df


def filter_orders_by_month(orders_df: pd.DataFrame) -> pd.DataFrame:
    mask = (
        (orders_df["order_date"].dt.year == TARGET_YEAR) &
        (orders_df["order_date"].dt.month == TARGET_MONTH)
    )
    filtered = orders_df[mask].copy()
    excluded = len(orders_df) - len(filtered)
    if excluded > 0:
        logger.info(f"Excluded {excluded} order(s) outside May 2024.")

    logger.info(f"{len(filtered)} order(s) remain after date filter (May 2024).")
    return filtered


def filter_negative_amounts(orders_df: pd.DataFrame) -> pd.DataFrame:
    df = orders_df.copy()

    # Convert to numeric; bad values become NaN
    df["order_amount"] = pd.to_numeric(df["order_amount"], errors="coerce")

    negative_or_null = df[df["order_amount"].isna() | (df["order_amount"] < 0)]
    if not negative_or_null.empty:
        logger.warning(
            f"Ignoring {len(negative_or_null)} order(s) with negative/invalid amounts: "
            f"{negative_or_null['order_id'].tolist()}"
        )

    df = df[df["order_amount"] >= 0].copy()
    return df


def aggregate_customer_orders(orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    # --- Count all valid orders per customer (regardless of status) ---
    order_counts = (
        orders_df.groupby("customer_id")
        .size()
        .reset_index(name="total_orders")
    )

    # --- Sum revenue for DELIVERED orders only ---
    delivered = orders_df[orders_df["order_status"] == "DELIVERED"]
    revenue = (
        delivered.groupby("customer_id")["order_amount"]
        .sum()
        .reset_index(name="total_spent")
    )

    # --- Log orders with unknown customer IDs ---
    known_ids = set(customers_df["customer_id"].astype(str))
    order_ids = set(orders_df["customer_id"].astype(str))
    unknown_ids = order_ids - known_ids
    if unknown_ids:
        logger.warning(f"Orders found for unknown customer IDs: {unknown_ids}")

    # --- Start with all customers to ensure zero-order customers appear ---
    result = customers_df[["customer_id"]].copy()
    result["customer_id"] = result["customer_id"].astype(str)

    result = result.merge(order_counts, on="customer_id", how="left")
    result = result.merge(revenue, on="customer_id", how="left")

    # Fill NaN with 0 for customers who had no matching orders
    result["total_orders"] = result["total_orders"].fillna(0).astype(int)
    result["total_spent"] = result["total_spent"].fillna(0.0).round(2)

    # Average order value = total_spent / total_orders (avoid divide by zero)
    result["average_order_value"] = result.apply(
        lambda row: round(row["total_spent"] / row["total_orders"], 2)
        if row["total_orders"] > 0 else 0.0,
        axis=1
    )

    logger.info(f"Aggregated metrics for {len(result)} customers.")
    return result


def process_orders(orders_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting order processing pipeline...")

    orders_df = parse_order_dates(orders_df)
    orders_df = filter_orders_by_month(orders_df)
    orders_df = filter_negative_amounts(orders_df)
    aggregated = aggregate_customer_orders(orders_df, customers_df)

    logger.info("Order processing pipeline complete.")
    return aggregated