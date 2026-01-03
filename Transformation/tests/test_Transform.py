import unittest
import pandas as pd
from Transformation.Transform import (
    json_to_dataframe,
    clean_dataFrame,
    parse_month_columns
)

class TestTransform(unittest.TestCase):

    def setUp(self):
        self.sample_json = {
            "SASTableData+P0142_7": [
                {
                    "H01": "P0142.7",
                    "H02": "Export and Import Unit Value Indices",
                    "MO012016": 63.1,
                    "MO022016": 62.7
                }
            ]
        }

    def test_parse_month_columns(self):
        self.assertEqual(parse_month_columns("MO012016"), "2016-01")
        self.assertEqual(parse_month_columns("MO022016"), "2016-02")
        self.assertEqual(parse_month_columns("H01"), "H01")

    def test_json_to_dataframe(self):
        df = json_to_dataframe(self.sample_json)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("MO012016", df.columns)

    def test_clean_dataframe_renames_columns(self):
        raw_df = json_to_dataframe(self.sample_json)
        cleaned_df = clean_dataFrame(raw_df)

        self.assertIn("2016-01", cleaned_df.columns)
        self.assertIn("2016-02", cleaned_df.columns)

    def test_clean_dataframe_values(self):
        raw_df = json_to_dataframe(self.sample_json)
        cleaned_df = clean_dataFrame(raw_df)

        self.assertEqual(cleaned_df.loc[0, "2016-01"], 63.1)
        self.assertEqual(cleaned_df.loc[0, "2016-02"], 62.7)

    
    def test_null_monthly_values_filled_with_zero(self):
        test_data = {
            "SASTableData+P0142_7": [
                {
                    "H01": "P0142.7",
                    "H02": "Export and Import Unit Value Indices",
                    "MO012016": None,
                    "MO022016": 62.7
                }
            ]
        }

        df = json_to_dataframe(test_data)
        cleaned_df = clean_dataFrame(df)

        self.assertEqual(cleaned_df.loc[0, "2016-01"], 0)
        self.assertEqual(cleaned_df.loc[0, "2016-02"], 62.7)



if __name__ == "__main__":
    unittest.main()
