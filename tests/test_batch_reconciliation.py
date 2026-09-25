import unittest

class TestBatchReconciliation(unittest.TestCase):
    def test_profitability_calculation(self):
        gross_earnings = 150.00
        fuel_cost = 45.00
        maintenance_cost = 25.00
        total_expenses = fuel_cost + maintenance_cost
        
        net_profit = round(gross_earnings - total_expenses, 2)
        profit_margin_pct = round((net_profit / gross_earnings) * 100, 2)
        is_unprofitable = net_profit < 0

        self.assertEqual(net_profit, 80.00)
        self.assertEqual(profit_margin_pct, 53.33)
        self.assertFalse(is_unprofitable)

    def test_unprofitable_vehicle_detection(self):
        gross_earnings = 95.00
        fuel_cost = 55.00
        maintenance_cost = 90.00
        total_expenses = fuel_cost + maintenance_cost
        
        net_profit = round(gross_earnings - total_expenses, 2)
        profit_margin_pct = round((net_profit / gross_earnings) * 100, 2)
        is_unprofitable = net_profit < 0

        self.assertEqual(net_profit, -50.00)
        self.assertLess(profit_margin_pct, 0)
        self.assertTrue(is_unprofitable)

if __name__ == '__main__':
    unittest.main()
