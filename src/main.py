import logging
import os
import sys


# Logging setup — must happen before any module imports that use logging
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/analytics.log"),   # write to file           
    ],
)

logger = logging.getLogger(__name__)

# Local module imports (after logging is configured)
from loader import load_customers, load_orders
from order_processor import process_orders
from loyalty_engine import apply_classifications
from reporter import generate_loyalty_report, generate_analytics_summary


# Path constants — adjust if your folder layout differs
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CUSTOMERS_FILE = os.path.join(BASE_DIR, "data", "customers.csv")
ORDERS_FILE    = os.path.join(BASE_DIR, "data", "orders.csv")

OUTPUT_REPORT  = os.path.join(BASE_DIR, "output", "customer_loyalty_report.csv")
OUTPUT_SUMMARY = os.path.join(BASE_DIR, "output", "analytics_summary.json")


def run_pipeline() -> None:
    logger.info("=" * 60)
    logger.info("Customer Analytics Pipeline — START")
    logger.info("=" * 60)
    print("Customer Order Analytics & Loyalty Classification Engine Starts")
    
    #Load raw data
    logger.info("Loading data files...")
    customers_df = load_customers(CUSTOMERS_FILE)
    orders_df    = load_orders(ORDERS_FILE)
    print("Data loaded")

    if customers_df.empty:
        logger.error("No customer data loaded. Aborting pipeline.")
        return

    if orders_df.empty:
        logger.warning("No order data loaded — all customers will have zero metrics.")

    
    #Process and aggregate orders
    logger.info("Processing and aggregating orders...")
    aggregated_df = process_orders(orders_df, customers_df)

    
    #Merge aggregations with customer master data
    logger.info("Merging customer data with aggregated metrics...")
    merged_df = customers_df.merge(aggregated_df, on="customer_id", how="left")

    # Fill any customers that didn't appear in aggregation (edge case)
    merged_df["total_orders"]        = merged_df["total_orders"].fillna(0).astype(int)
    merged_df["total_spent"]         = merged_df["total_spent"].fillna(0.0)
    merged_df["average_order_value"] = merged_df["average_order_value"].fillna(0.0)
    print("Data processed and merged")
    
    #Apply loyalty and activity classifications
    logger.info("Applying loyalty and activity classifications...")
    classified_df = apply_classifications(merged_df)
    print("Classifications applied")
    
    # Step 5 — Generate output files
    logger.info("Generating output files...")
    generate_loyalty_report(classified_df, OUTPUT_REPORT)
    generate_analytics_summary(classified_df, OUTPUT_SUMMARY)
    print("Output files generated")

    logger.info("=" * 60)
    logger.info("Customer Analytics Pipeline — COMPLETE")
    logger.info(f"  Loyalty Report : {OUTPUT_REPORT}")
    logger.info(f"  Summary JSON   : {OUTPUT_SUMMARY}")
    logger.info(f"  Log File       : logs/analytics.log")
    logger.info("=" * 60)
    print("Customer Order Analytics & Loyalty Classification Engine Completed")
    print(f"Report: {OUTPUT_REPORT}")
    print(f"Summary: {OUTPUT_SUMMARY}")
    print(f"Log file: logs/analytics.log")
    print("\n")
    
if __name__ == "__main__":
    run_pipeline()