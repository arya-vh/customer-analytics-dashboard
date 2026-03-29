import pandas as pd
import json
import logging
import os

logger = logging.getLogger(__name__)

# Columns required in the final loyalty report CSV
REPORT_COLUMNS = [
    "customer_id",
    "customer_name",
    "total_orders",
    "total_spent",
    "average_order_value",
    "loyalty_segment",
    "customer_activity_status",
]


def generate_loyalty_report(classified_df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Validate all required columns are present
    missing = [col for col in REPORT_COLUMNS if col not in classified_df.columns]
    if missing:
        logger.error(f"Cannot generate report — missing columns: {missing}")
        return

    report_df = classified_df[REPORT_COLUMNS].copy()
    report_df.to_csv(output_path, index=False)
    logger.info(f"Loyalty report written to: {output_path} ({len(report_df)} rows)")


def generate_analytics_summary(classified_df: pd.DataFrame, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    summary = {
        "total_customers": int(len(classified_df)),
        "active_customers": int(
            (classified_df["customer_activity_status"] == "ACTIVE_CUSTOMER").sum()
        ),
        "inactive_customers": int(
            (classified_df["customer_activity_status"] == "INACTIVE_CUSTOMER").sum()
        ),
        "platinum_customers": int(
            (classified_df["loyalty_segment"] == "PLATINUM").sum()
        ),
        "gold_customers": int(
            (classified_df["loyalty_segment"] == "GOLD").sum()
        ),
        "silver_customers": int(
            (classified_df["loyalty_segment"] == "SILVER").sum()
        ),
        "bronze_customers": int(
            (classified_df["loyalty_segment"] == "BRONZE").sum()
        ),
        "total_revenue": round(float(classified_df["total_spent"].sum()), 2),
    }

    with open(output_path, "w") as f:
        json.dump(summary, f, indent=4)

    logger.info(f"Analytics summary written to: {output_path}")
    logger.info(f"Summary: {summary}")