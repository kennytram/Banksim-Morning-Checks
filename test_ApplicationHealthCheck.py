from unittest import TestCase
from unittest.mock import patch

from ApplicationHealthCheck import (
    ApplicationHealthCheck,
    TBAHealthCheck,
    PMAHealthCheck,
    CRSHealthCheck,
)


class TestApplicationHealthCheck(TestCase):
    def setUp(self):
        self.base_dir = "/mock/base/dir"
        self.business_date = "20240905"
        self.app_health_check = ApplicationHealthCheck(
            self.base_dir, self.business_date
        )

    @patch("ApplicationHealthCheck.FileChecker")
    def test_find_missing_files(self, MockFileChecker):
        mock_file_checker = MockFileChecker.return_value
        mock_file_checker.find_phrases.return_value = {}
        self.app_health_check.file_checker = mock_file_checker

        self.app_health_check.dirs = {
            "input": "/mock/path/to/input",
            "output": "/mock/path/to/output",
            "logs": "/mock/path/to/logs",
        }

        self.app_health_check.file_categories_patterns = {
            "input": ["missing_file.csv"],
        }

        mock_file_checker.get_files.return_value = set()
        missing_files = self.app_health_check.find_missing_files()
        self.assertIn("/mock/path/to/input/missing_file.csv", missing_files)


class TestTBAHealthCheck(TestCase):
    def setUp(self):
        self.base_dir = "/mock/base/dir"
        self.business_date = "20240905"
        self.tba_health_check = TBAHealthCheck(self.base_dir, self.business_date)

    @patch("ApplicationHealthCheck.FileChecker")
    def test_calculate_received(self, MockFileChecker):
        mock_file_checker = MockFileChecker.return_value
        mock_file_checker.count_csv_rows_matching_files.return_value = 10
        self.tba_health_check.file_checker = mock_file_checker

        self.tba_health_check.calculate_received()
        self.assertEqual(self.tba_health_check.trade_data["total"], 30)


class TestPMAHealthCheck(TestCase):
    def setUp(self):
        self.base_dir = "/mock/base/dir"
        self.business_date = "20240905"
        self.pma_health_check = PMAHealthCheck(self.base_dir, self.business_date)

    @patch("ApplicationHealthCheck.FileChecker")
    def test_calculate_received(self, MockFileChecker):
        mock_file_checker = MockFileChecker.return_value
        mock_file_checker.count_csv_rows_matching_files.return_value = 15
        self.pma_health_check.file_checker = mock_file_checker

        self.pma_health_check.calculate_received()
        self.assertEqual(self.pma_health_check.trade_data["total"], 45)


class TestCRSHealthCheck(TestCase):
    def setUp(self):
        self.base_dir = "/mock/base/dir"
        self.business_date = "20240905"
        self.crs_health_check = CRSHealthCheck(self.base_dir, self.business_date)

    @patch("ApplicationHealthCheck.FileChecker")
    def test_calculate_received(self, MockFileChecker):
        mock_file_checker = MockFileChecker.return_value
        mock_file_checker.count_csv_rows_matching_files_matching_columns.return_value = (
            20
        )
        self.crs_health_check.file_checker = mock_file_checker

        self.crs_health_check.calculate_received()
        self.assertEqual(self.crs_health_check.trade_data["total"], 40)
