import os
import glob
import csv


class FileChecker:

    def __init__(self, base_dir: str = "./"):
        self.base_dir = base_dir
        self.file_paths = set()

    def get_files(self, dir: str, file_patterns: set) -> set:
        file_paths = set()
        for file_pattern in file_patterns:
            matched_files = glob.glob(os.path.join(dir, file_pattern), recursive=True)
            file_paths.update(matched_files)
        return file_paths

    def get_num_files(self, dir: str, file_pattern: str) -> int:
        return len(glob.glob(os.path.join(dir, file_pattern), recursive=True))

    def find_occurrences(self, keyword: str, files: set = set()) -> dict:
        if not files:
            return {}

        occurrences = dict()

        for file_path in files:
            with open(file_path, "r") as file:
                matching_lines = [line for line in file if keyword in line]
                if matching_lines:
                    occurrences[file_path] = matching_lines

        return occurrences

    def find_phrases(self, files: set, phrases: list) -> dict:
        found_pharses = dict()

        for file_path in files:
            with open(file_path, "r") as file:
                for line in file:
                    for phrase in phrases:
                        if phrase in line:
                            next_word = line.strip().split(phrase)[1].split()[0]
                            if file_path not in found_pharses:
                                found_pharses[file_path] = []
                            found_pharses[file_path].append(next_word)

        return dict(found_pharses)

    def count_csv_rows_matching_files(self, pattern: str) -> int:
        matching_files = glob.glob(pattern)

        total_lines = 0
        for file_path in matching_files:
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    total_lines += sum(1 for _ in file) - 1

        return total_lines

    def count_csv_rows_matching_files_matching_columns(
        self, file_pattern: str, column_pattern: str, column_idx: int
    ) -> int:
        matching_files = glob.glob(file_pattern)

        total_lines = 0
        for file_path in matching_files:
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    reader = csv.reader(file)
                    for row in reader:
                        if row and row[column_idx] == column_pattern:
                            total_lines += 1
        return total_lines
