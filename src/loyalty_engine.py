import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Loyalty thresholds (in currency units)
PLATINUM_THRESHOLD = 10000
GOLD_THRESHOLD = 5000
SILVER_THRESHOLD = 1000


def classify_loyalty_segment(total_spent: float) -> str:
    if total_spent >= PLATINUM_THRESHOLD:
        return "PLATINUM"
    elif total_spent >= GOLD_THRESHOLD:
        return "GOLD"
    elif total_spent >= SILVER_THRESHOLD:
        return "SILVER"
    else:
        return "BRONZE"


def classify_activity_status(original_status: str, total_orders: int) -> str:
    if original_status == "ACTIVE" and total_orders > 0:
        return "ACTIVE_CUSTOMER"
    else:
        return "INACTIVE_CUSTOMER"


def apply_classifications(merged_df: pd.DataFrame) -> pd.DataFrame:
    df = merged_df.copy()

    df["loyalty_segment"] = df["total_spent"].apply(classify_loyalty_segment)

    df["customer_activity_status"] = df.apply(
        lambda row: classify_activity_status(row["status"], row["total_orders"]),
        axis=1
    )

    active_count = (df["customer_activity_status"] == "ACTIVE_CUSTOMER").sum()
    inactive_count = (df["customer_activity_status"] == "INACTIVE_CUSTOMER").sum()
    logger.info(
        f"Classification complete — "
        f"Active: {active_count}, Inactive: {inactive_count} | "
        f"Platinum: {(df['loyalty_segment'] == 'PLATINUM').sum()}, "
        f"Gold: {(df['loyalty_segment'] == 'GOLD').sum()}, "
        f"Silver: {(df['loyalty_segment'] == 'SILVER').sum()}, "
        f"Bronze: {(df['loyalty_segment'] == 'BRONZE').sum()}"
    )

    return df