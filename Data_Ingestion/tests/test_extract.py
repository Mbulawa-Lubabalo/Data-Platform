import unittest
from testData import rawData


class TestFetchedData(unittest.TestCase):
    def setUp(self):
        self.data = rawData["SASTableData+P0142_7"]

    def test_data_is_not_empty(self):
        self.assertTrue(len(self.data) > 0)

    def test_data_is_list(self):
        self.assertIsInstance(self.data, list)