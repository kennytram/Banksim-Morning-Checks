import pytest
import os
import glob
import csv
from unittest.mock import mock_open
from FileChecker import FileChecker

@pytest.fixture
def file_checker() -> FileChecker:
    return FileChecker(base_dir="/test/base")

def test_find_occurrences(file_checker, monkeypatch):
    files = {"/test/dir/file1.log", "/test/dir/file2.log"}
    keyword = "test"

    m_open = mock_open(read_data="this is a test line")
    monkeypatch.setattr("builtins.open", m_open)

    result = file_checker.find_occurrences(keyword, files)

    expected = {
        "/test/dir/file1.log": ["this is a test line"],
        "/test/dir/file2.log": ["this is a test line"]
    }
    assert result == expected

def test_find_phrases(file_checker, monkeypatch):
    files = {"/test/dir/file1.log"}
    phrases = ["test", "example"]

    m_open = mock_open(read_data="this is a test phrase\nanother example phrase")
    monkeypatch.setattr("builtins.open", m_open)

    result = file_checker.find_phrases(files, phrases)

    expected = {
        "/test/dir/file1.log": ["phrase", "phrase"]
    }
    assert result == expected