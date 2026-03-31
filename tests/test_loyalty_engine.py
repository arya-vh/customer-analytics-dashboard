import sys
import os
import unittest
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from loyalty_engine import (
    classify_loyalty_segment,
    classify_activity_status,
    apply_classifications,
)


# Loyalty Segment Classification Tests
class TestLoyaltyClassification(unittest.TestCase):

    def test_platinum_customer_classification(self):
        """total_spent >= 10000 - PLATINUM"""
        self.assertEqual(classify_loyalty_segment(10000), "PLATINUM")

    def test_platinum_customer_well_above_threshold(self):
        """Values well above the threshold must still be PLATINUM."""
        self.assertEqual(classify_loyalty_segment(50000), "PLATINUM")

    def test_platinum_customer_exact_boundary(self):
        """Exactly 10000 is PLATINUM (boundary check)."""
        self.assertEqual(classify_loyalty_segment(10000.00), "PLATINUM")


    def test_gold_customer_classification(self):
        """total_spent >= 5000 and < 10000 - GOLD"""
        self.assertEqual(classify_loyalty_segment(5000), "GOLD")

    def test_gold_customer_mid_range(self):
        self.assertEqual(classify_loyalty_segment(7500), "GOLD")

    def test_gold_customer_just_below_platinum(self):
        """9999.99 is just below PLATINUM — must be GOLD."""
        self.assertEqual(classify_loyalty_segment(9999.99), "GOLD")


    def test_silver_customer_classification(self):
        """total_spent >= 1000 and < 5000 - SILVER"""
        self.assertEqual(classify_loyalty_segment(1000), "SILVER")

    def test_silver_customer_mid_range(self):
        self.assertEqual(classify_loyalty_segment(3000), "SILVER")

    def test_silver_customer_just_below_gold(self):
        """4999.99 is just below GOLD — must be SILVER."""
        self.assertEqual(classify_loyalty_segment(4999.99), "SILVER")


    def test_bronze_customer_classification(self):
        self.assertEqual(classify_loyalty_segment(999.99), "BRONZE")

    def test_bronze_customer_zero_spend(self):
        self.assertEqual(classify_loyalty_segment(0), "BRONZE")

    def test_bronze_customer_small_amount(self):
        self.assertEqual(classify_loyalty_segment(500), "BRONZE")


# Activity Status Classification Tests
class TestActivityStatus(unittest.TestCase):

    def test_active_customer_with_orders(self):
        result = classify_activity_status("ACTIVE", total_orders=3)
        self.assertEqual(result, "ACTIVE_CUSTOMER")

    def test_active_customer_with_single_order(self):
        result = classify_activity_status("ACTIVE", total_orders=1)
        self.assertEqual(result, "ACTIVE_CUSTOMER")

    def test_active_customer_without_orders(self):
        result = classify_activity_status("ACTIVE", total_orders=0)
        self.assertEqual(result, "INACTIVE_CUSTOMER")

    def test_inactive_customer_status(self):
        result = classify_activity_status("INACTIVE", total_orders=5)
        self.assertEqual(result, "INACTIVE_CUSTOMER")

    def test_inactive_customer_with_zero_orders(self):
        result = classify_activity_status("INACTIVE", total_orders=0)
        self.assertEqual(result, "INACTIVE_CUSTOMER")


# Integration Tests — apply_classifications DataFrame API
class TestApplyClassifications(unittest.TestCase):

    def _make_df(self, rows):
        return pd.DataFrame(rows)

    def test_output_columns_present(self):
        df = self._make_df([{
            "customer_id": "C001",
            "customer_name": "Alice",
            "email": "a@test.com",
            "status": "ACTIVE",
            "signup_date": "2024-01-01",
            "total_orders": 2,
            "total_spent": 6000.0,
            "average_order_value": 3000.0,
        }])
        result = apply_classifications(df)
        self.assertIn("loyalty_segment", result.columns)
        self.assertIn("customer_activity_status", result.columns)

    def test_full_row_classification(self):
        df = self._make_df([{
            "customer_id": "C002",
            "customer_name": "Bob",
            "email": "b@test.com",
            "status": "ACTIVE",
            "signup_date": "2024-01-01",
            "total_orders": 5,
            "total_spent": 12000.0,
            "average_order_value": 2400.0,
        }])
        result = apply_classifications(df)
        row = result.iloc[0]
        self.assertEqual(row["loyalty_segment"], "PLATINUM")
        self.assertEqual(row["customer_activity_status"], "ACTIVE_CUSTOMER")

    def test_inactive_zero_spend_classification(self):
        df = self._make_df([{
            "customer_id": "C003",
            "customer_name": "Carol",
            "email": "c@test.com",
            "status": "INACTIVE",
            "signup_date": "2023-06-01",
            "total_orders": 0,
            "total_spent": 0.0,
            "average_order_value": 0.0,
        }])
        result = apply_classifications(df)
        row = result.iloc[0]
        self.assertEqual(row["loyalty_segment"], "BRONZE")
        self.assertEqual(row["customer_activity_status"], "INACTIVE_CUSTOMER")


if __name__ == "__main__":
    unittest.main(verbosity=2)