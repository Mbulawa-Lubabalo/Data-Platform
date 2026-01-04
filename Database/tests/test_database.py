import os
import unittest
from unittest.mock import patch, MagicMock
from sqlalchemy import Engine
from Database.connection import Database_Connection


class TestDatabaseConnection(unittest.TestCase):

    def test_no_database_url(self):
        """Should raise RuntimeError if DATABASE_URL is not set"""
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as context:
                Database_Connection()
            self.assertEqual(str(context.exception), "DATABASE_URL environment variable not set")

    @patch("Database.connection.create_engine")
    def test_database_url_set(self, mock_create_engine):
        """Should return a SQLAlchemy engine if DATABASE_URL is set"""
        mock_engine = MagicMock(spec=Engine)
        mock_create_engine.return_value = mock_engine

        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@localhost/db"}):
            engine = Database_Connection()

        mock_create_engine.assert_called_once_with("postgresql://user:pass@localhost/db", pool_pre_ping=True)
        self.assertEqual(engine, mock_engine)

if __name__ == "__main__":
    unittest.main()