import pytest
from FileWriter import FileWriter


@pytest.fixture
def file_writer() -> FileWriter:
    return FileWriter("Test Data")


def test_write(file_writer: FileWriter, tmp_path) -> None:
    temp_file = tmp_path / "output.txt"

    file_writer.write("output.txt", tmp_path)

    assert temp_file.exists(), "File was not created."

    with open(temp_file, "r") as f:
        content = f.read()
    assert content == "Test Data", f"Unexpected file content: {content}"
