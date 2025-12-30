import unittest
from unittest.mock import patch
from extract import*


class TestFetchedData(unittest.TestCase):

    @patch("extract.requests.get")
    def test_fetch_data_success(self, mock_get):
        # Mock response object
        mock_response = mock_get.return_value
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "SASJSONExport": "1.0 PRETTY",
            "SASTableData+P0142_7": [
                {
                    "H01": "P0142.7",
                    "H02": "Export and Import Unit Value Indices",
                    "MO012016": 63.1,
                    "MO022016": 62.7
                }
            ]
        }

        result = Fetch_Data()

        # Assertions
        self.assertIsInstance(result, dict)
        self.assertIn("SASTableData+P0142_7", result)
        self.assertTrue(len(result["SASTableData+P0142_7"]) > 0)

    
    @patch("extract.requests.get")
    def test_fetch_data_http_error(self, mock_get):
        mock_get.return_value.raise_for_status.side_effect = Exception("HTTP Error")

        with self.assertRaises(Exception):
            Fetch_Data()
