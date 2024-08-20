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

    # def get_dir(self, *args: str) -> str:
    #     dir = os.path.join(self.base_dir, *args)
    #     return dir

    # def filter_files_with_pattern(self, files: set, pattern: str) -> set:

    #     matching_files = []
    #     for file_name in files:
    #         if pattern in file_name:
    #             matching_files.append(file_name)
    #     return matching_files

    # def find_older_files(self, dir: str, date: str) -> set:
    #     older_files = []
    #     business_date_obj = datetime.strptime(date, "%Y%m%d")
    #     for file_name in dir:
    #         date_str = file_name.split('_')[-1].split('.')[0]

    #         if self.is_valid_date(date_str):
    #             file_date_obj = datetime.strptime(date_str, "%Y%m%d")
    #             if file_date_obj < business_date_obj:
    #                 older_files.append(file_name)
    #     return older_files

    # def count_csv_rows(file_path: str) -> int:
    #     with open(file_path, "r") as file:
    #         data = csv.reader(file)
    #         data_rows = sum(1 for _ in data) - 1

    #     return data_rows

    # def get_file_size(self, file_path: str) -> int:
    #     return os.path.getsize(file_path)

    # def extract_date_file_name(self, file_name: str):
    #     if file_name[0] == '.':
    #         file_name = file_name.split(".")[1]
    #     date = file_name.split(".")[0].split("_")[-1]
    #     return date

    # def get_files_dates(self, file_paths: list) -> list:
    #     dates = set()
    #     for file in file_paths:
    #         if os.path.isfile(file):
    #             date_str = self.extract_date_file_name(file)
    #             if self.is_valid_date(date_str):
    #                 dates.add(date_str)
    #     return dates

    # def is_valid_date(self, date_str: str) -> bool:
    #     try:
    #         datetime.strptime(date_str, "%Y%m%d")
    #         return True
    #     except ValueError:
    #         return False

    # def find_earliest_date(self, date_strings: set) -> datetime:
    #     dates = [datetime.strptime(date_str, "%Y%m%d") for date_str in date_strings]
    #     return min(dates)

    # def compare_date_with_string(date_obj: datetime, date_str: str) -> bool:
    #     try:
    #         date_from_string = datetime.strptime(date_str, "%Y%m%d")
    #     except ValueError:
    #         raise ValueError(f"Date string '{date_str}' is not in the correct format.")
    #     return date_obj == date_from_string

    # def change_files(self, files: set) -> None:
    #     self.files = files
