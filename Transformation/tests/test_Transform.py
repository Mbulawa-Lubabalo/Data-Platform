import unittest
import pandas as pd
from datetime import date

from Transformation.Transform import transform_json_to_df


class TestTransform(unittest.TestCase):

    def setUp(self):
        self.sample_json = [
            {
                "H01": "P0142.7",
                "H02": "Export and Import Unit Value Indices",
                "H03": "UVI43100",
                "H04": "Exports",
                "H05": "Coal",
                "H17": "Index",
                "H18": "December 2020 =100",
                "H25": "Monthly",
                "MO012016": 63.1,
                "MO022016": 62.7
            }
        ]

    def test_transform_returns_dataframe(self):
        df = transform_json_to_df(self.sample_json)
        self.assertIsInstance(df, pd.DataFrame)

    def test_row_explosion(self):
        df = transform_json_to_df(self.sample_json)
        # 2 months → 2 rows
        self.assertEqual(len(df), 2)

    def test_dimension_columns_present(self):
        df = transform_json_to_df(self.sample_json)

        expected_columns = {
            "series_code",
            "series_name",
            "frequency",
            "base_period",
            "indicator_code",
            "category",
            "subcategory",
            "unit",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "year_month",
            "index_value"
        }

        self.assertTrue(expected_columns.issubset(df.columns))

    def test_date_parsing(self):
        df = transform_json_to_df(self.sample_json)

        self.assertIn(date(2016, 1, 1), df["date"].values)
        self.assertIn(date(2016, 2, 1), df["date"].values)

    def test_index_values_correct(self):
        df = transform_json_to_df(self.sample_json)

        jan_value = df.loc[df["year_month"] == "2016-01", "index_value"].iloc[0]
        feb_value = df.loc[df["year_month"] == "2016-02", "index_value"].iloc[0]

        self.assertEqual(jan_value, 63.1)
        self.assertEqual(feb_value, 62.7)

    def test_quarter_calculation(self):
        df = transform_json_to_df(self.sample_json)

        jan_quarter = df.loc[df["month"] == 1, "quarter"].iloc[0]
        feb_quarter = df.loc[df["month"] == 2, "quarter"].iloc[0]

        self.assertEqual(jan_quarter, 1)
        self.assertEqual(feb_quarter, 1)

    # def test_null_month_values_are_excluded(self):
    #     test_data = [
    #         {
    #             "H01": "P0142.7",
    #             "H02": "Export and Import Unit Value Indices",
    #             "H03": "UVI43100",
    #             "H04": "Exports",
    #             "H05": "Coal",
    #             "H17": "Index",
    #             "H18": "December 2020 =100",
    #             "H25": "Monthly",
    #             "MO012016": None,
    #             "MO022016": 62.7
    #         }
    #     ]

    #     df = transform_json_to_df(test_data)

    #     self.assertEqual(len(df), 1)
    #     self.assertEqual(df.iloc[0]["year_month"], "2016-02")
    #     self.assertEqual(df.iloc[0]["index_value"], 62.7)


if __name__ == "__main__":
    unittest.main()
