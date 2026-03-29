import sys
import os
import unittest
import pandas as pd

# Make src/ importable when running tests from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from order_processor import (
    parse_order_dates,
    filter_orders_by_month,
    filter_negative_amounts,
    aggregate_customer_orders,
    process_orders,
)


def make_customers(*customer_ids):
    """Return a customers DataFrame with the given IDs (all ACTIVE)."""
    return pd.DataFrame({
        "customer_id": list(customer_ids),
        "customer_name": [f"Customer {cid}" for cid in customer_ids],
        "email": [f"{cid}@test.com" for cid in customer_ids],
        "status": ["ACTIVE"] * len(customer_ids),
        "signup_date": ["2024-01-01"] * len(customer_ids),
    })


def make_order(order_id, customer_id, order_date, order_amount, order_status):
    """Return a single-row orders DataFrame."""
    return pd.DataFrame([{
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date,
        "order_amount": order_amount,
        "order_status": order_status,
    }])


# Aggregation Tests
class TestAggregation(unittest.TestCase):

    def test_customer_order_aggregation(self):
        customers = make_customers("C001")
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C001", "order_date": "2024-05-01",
             "order_amount": 500.0, "order_status": "DELIVERED"},
            {"order_id": "O2", "customer_id": "C001", "order_date": "2024-05-10",
             "order_amount": 300.0, "order_status": "CANCELLED"},
            {"order_id": "O3", "customer_id": "C001", "order_date": "2024-05-15",
             "order_amount": 200.0, "order_status": "RETURNED"},
        ])
        orders["order_date"] = pd.to_datetime(orders["order_date"])
        orders["order_amount"] = orders["order_amount"].astype(float)

        result = aggregate_customer_orders(orders, customers)
        row = result[result["customer_id"] == "C001"].iloc[0]

        self.assertEqual(row["total_orders"], 3)          # all three count
        self.assertAlmostEqual(row["total_spent"], 500.0) # only DELIVERED
        # avg = 500 / 3 ≈ 166.67
        self.assertAlmostEqual(row["average_order_value"], round(500.0 / 3, 2))

    def test_customer_with_no_orders_has_zero_metrics(self):
        customers = make_customers("C001", "C002")
        # C002 has no orders
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C001", "order_date": "2024-05-01",
             "order_amount": 100.0, "order_status": "DELIVERED"},
        ])
        orders["order_date"] = pd.to_datetime(orders["order_date"])

        result = aggregate_customer_orders(orders, customers)

        self.assertEqual(len(result), 2)          # both customers present
        c002 = result[result["customer_id"] == "C002"].iloc[0]
        self.assertEqual(c002["total_orders"], 0)
        self.assertEqual(c002["total_spent"], 0.0)
        self.assertEqual(c002["average_order_value"], 0.0)

    def test_invalid_order_date_ignored(self):
        customers = make_customers("C001")
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C001",
             "order_date": "NOT-A-DATE",          # <-- invalid
             "order_amount": 999.0, "order_status": "DELIVERED"},
            {"order_id": "O2", "customer_id": "C001",
             "order_date": "2024-05-20",           # valid
             "order_amount": 400.0, "order_status": "DELIVERED"},
        ])

        result = process_orders(orders, customers)
        row = result[result["customer_id"] == "C001"].iloc[0]

        # Only the valid May 2024 order should be counted
        self.assertEqual(row["total_orders"], 1)
        self.assertAlmostEqual(row["total_spent"], 400.0)

    def test_negative_order_amount_ignored(self):
        customers = make_customers("C001")
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C001",
             "order_date": "2024-05-01",
             "order_amount": -50.0,       # negative, must be ignored
             "order_status": "DELIVERED"},
            {"order_id": "O2", "customer_id": "C001",
             "order_date": "2024-05-01",
             "order_amount": 250.0,       # valid
             "order_status": "DELIVERED"},
        ])

        result = process_orders(orders, customers)
        row = result[result["customer_id"] == "C001"].iloc[0]

        self.assertEqual(row["total_orders"], 1)
        self.assertAlmostEqual(row["total_spent"], 250.0)

    def test_only_may_2024_orders_included(self):
        customers = make_customers("C001")
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C001",
             "order_date": "2024-04-30",   # April — excluded
             "order_amount": 500.0, "order_status": "DELIVERED"},
            {"order_id": "O2", "customer_id": "C001",
             "order_date": "2024-05-01",   # May — included
             "order_amount": 200.0, "order_status": "DELIVERED"},
            {"order_id": "O3", "customer_id": "C001",
             "order_date": "2024-06-01",   # June — excluded
             "order_amount": 700.0, "order_status": "DELIVERED"},
        ])

        result = process_orders(orders, customers)
        row = result[result["customer_id"] == "C001"].iloc[0]

        self.assertEqual(row["total_orders"], 1)
        self.assertAlmostEqual(row["total_spent"], 200.0)


# Date Parsing Tests
class TestDateParsing(unittest.TestCase):

    def test_valid_dates_are_parsed(self):
        """Rows with valid dates survive parse_order_dates."""
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C1",
             "order_date": "2024-05-01", "order_amount": 100, "order_status": "DELIVERED"},
        ])
        result = parse_order_dates(orders)
        self.assertEqual(len(result), 1)
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["order_date"]))

    def test_invalid_dates_are_dropped(self):
        """Rows with bad dates are removed after parse_order_dates."""
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C1",
             "order_date": "INVALID", "order_amount": 100, "order_status": "DELIVERED"},
            {"order_id": "O2", "customer_id": "C2",
             "order_date": "2024-05-10", "order_amount": 200, "order_status": "DELIVERED"},
        ])
        result = parse_order_dates(orders)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["order_id"], "O2")


# Amount Filtering Tests
class TestAmountFiltering(unittest.TestCase):

    def test_zero_amount_is_kept(self):
        """Zero is a valid (non-negative) amount and should NOT be filtered out."""
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C1",
             "order_date": "2024-05-01", "order_amount": 0, "order_status": "DELIVERED"},
        ])
        result = filter_negative_amounts(orders)
        self.assertEqual(len(result), 1)

    def test_negative_amounts_are_dropped(self):
        """Negative amounts must be removed."""
        orders = pd.DataFrame([
            {"order_id": "O1", "customer_id": "C1",
             "order_date": "2024-05-01", "order_amount": -1, "order_status": "DELIVERED"},
            {"order_id": "O2", "customer_id": "C2",
             "order_date": "2024-05-01", "order_amount": 100, "order_status": "DELIVERED"},
        ])
        result = filter_negative_amounts(orders)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["order_id"], "O2")


if __name__ == "__main__":
    unittest.main(verbosity=2)