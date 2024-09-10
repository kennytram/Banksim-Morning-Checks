import pytest
import os
import tarfile
from TarManager import TarManager


@pytest.fixture
def tar_manager() -> TarManager:
    return TarManager('/test/dir')


def test_get_file_patterns_paths(tar_manager: TarManager, monkeypatch) -> None:
    test_dir = "/test/dir"
    test_patterns = {"*.log", "*.xlsx", "*.csv", ".txt", ".xls", ".py"}

    def mock_glob(pattern, recursive=True):
        if pattern == "/test/dir/*.log":
            return ["/test/dir/file1.log"]
        elif pattern == "/test/dir/*.xlsx":
            return ["/test/dir/file2.xlsx"]
        elif pattern == "/test/dir/*.csv":
            return ["/test/dir/file3.csv"]
        return []

    monkeypatch.setattr("glob.glob", mock_glob)

    result = tar_manager.get_file_patterns_paths(test_dir, test_patterns)

    expected_result = {
        "/test/dir/file1.log",
        "/test/dir/file2.xlsx",
        "/test/dir/file3.csv",
    }

    assert result == expected_result

def test_tar_creation(tar_manager, monkeypatch):
    tar_name = "output"
    file_paths = {"/path/to/file1.log", "/path/to/file2.log"}

    mock_tarfile = None
    files = []

    class MockTarFile:
        def __init__(self, tar_name, mode):
            self.tar_name = tar_name
            self.mode = mode

        def add(self, file_path, arcname=None):
            files.append((file_path, arcname))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    def mock_makedirs(path, exist_ok):
        assert path == "/test/dir"
        assert exist_ok is True
    monkeypatch.setattr(os, "makedirs", mock_makedirs)

    def mock_tarfile_open(name, mode):
        nonlocal mock_tarfile
        mock_tarfile = MockTarFile(name, mode)
        return mock_tarfile

    monkeypatch.setattr(tarfile, "open", mock_tarfile_open)

    monkeypatch.setattr(os.path, "basename", lambda file_path: file_path.split('/')[-1])

    tar_manager.tar(tar_name, file_paths)

    expected_tar_name = "/test/dir/output.tar.gz"

    assert mock_tarfile.tar_name == expected_tar_name
    assert mock_tarfile.mode == "w:gz"

    expected_files = [("/path/to/file1.log", "file1.log"), ("/path/to/file2.log", "file2.log")]
    assert files == expected_files or files == expected_files[::-1]

def test_set_source_dir(tar_manager):
    source_dir = "/test/source"
    tar_manager.set_source_dir(source_dir)
    assert tar_manager.source_dir == source_dir

def test_set_dest_dir(tar_manager):
    dest_dir = "/test/destination"
    tar_manager.set_dest_dir(dest_dir)
    assert tar_manager.dest_dir == dest_dir

def test_archive_success(tar_manager, monkeypatch):
    dir = "/test/source"
    tar_name = "output.tar.gz"
    file_patterns = {"*.log", "*.csv"}

    expected_paths = {"/test/source/file1.log", "/test/source/file2.csv"}
    monkeypatch.setattr(tar_manager, "get_file_patterns_paths", lambda d, p: expected_paths)

    def mock_tar(name, paths):
        assert name == tar_name
        assert paths == expected_paths
    monkeypatch.setattr(tar_manager, "tar", mock_tar)

    result = tar_manager.archive(dir, tar_name, file_patterns)
    assert result is True

def test_archive_failure(tar_manager, monkeypatch):
    dir = "/test/source"
    tar_name = "output.tar.gz"
    file_patterns = {"*.log", "*.xls"}

    monkeypatch.setattr(tar_manager, "get_file_patterns_paths", lambda d, p: Exception("Error"))

    result = tar_manager.archive(dir, tar_name, file_patterns)
    assert result is False