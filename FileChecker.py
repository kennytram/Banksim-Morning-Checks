import os
import glob
import csv


class FileChecker:

    def __init__(self, base_dir: str = "./"):
        self.base_dir = base_dir
        self.file_paths = set()

    def get_dir(self, *args: str) -> str:
        dir = os.path.join(self.base_dir, *args)
        return dir

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
    
    def count_csv_rows(file_path: str) -> int:
        with open(file_path, 'r') as file:
            data = csv.reader(file)
            data_rows = sum(1 for _ in data) - 1
        
        return data_rows
    
    def count_csv_rows_matching_files(self, pattern: str) -> int:
        matching_files = glob.glob(pattern)

        total_lines = 0
        for file_path in matching_files:
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    total_lines += sum(1 for _ in file) - 1

        return total_lines
    
    def count_csv_rows_matching_files_matching_columns(self, file_pattern: str, column_pattern: str, column_idx: int) -> int:
        matching_files = glob.glob(file_pattern)
        
        total_lines = 0
        for file_path in matching_files:
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        if row and row[column_idx] == column_pattern:
                            total_lines += 1
        return total_lines

    def get_file_size(self, file_path):
        return

    def change_files(self, files: set) -> None:
        self.files = files

    # def __init__(self, base_dir: str, business_date):
    #     self.base_dir = base_dir
    #     self.business_date = business_date

    # def read(self):
    #     # Directories
    #     self.dir_banksimlogs = os.path.join(base_dir, "banksimlogs", business_date)
    #     self.dir_input_crs = os.path.join(base_dir, "crs", "data", "input")
    #     self.dir_input_pma = os.path.join(base_dir, "pma", "data", "input")
    #     self.dir_input_tba = os.path.join(base_dir, "tba", "data", "input")
    #     self.dir_output_crs = os.path.join(base_dir, "crs", "data", "output")
    #     self.dir_output_pma = os.path.join(base_dir, "pma", "data", "output")
    #     self.dir_output_tba = os.path.join(base_dir, "tba", "data", "output")

    #     # Count
    #     self.count_files()

    # def count_files(self):
    #     # Count log files
    #     self.banksim_logs_files = sum(
    #         len(glob.glob(os.path.join(self.dir_banksimlogs, subdir, "*.log")))
    #         for subdir in ["crs", "pma", "tba"]
    #     )

    #     # Count input files
    #     self.input_files_crs = len(
    #         glob.glob(os.path.join(self.dir_input_crs, f"*{self.business_date}*.csv"))
    #     )
    #     self.input_files_pma = len(
    #         glob.glob(os.path.join(self.dir_input_pma, f"*{self.business_date}*.csv"))
    #     )
    #     self.input_files_tba = len(
    #         glob.glob(os.path.join(self.dir_input_tba, f"*{self.business_date}*.csv"))
    #     )

    #     # Count output files
    #     self.output_files_crs = len(
    #         glob.glob(os.path.join(self.dir_output_crs, "risk_dataset.xls"))
    #     )
    #     self.output_files_pma = len(
    #         glob.glob(os.path.join(self.dir_output_pma, f"*{self.business_date}*.csv"))
    #     )
    #     self.output_files_tba = len(
    #         glob.glob(os.path.join(self.dir_output_tba, f"*{self.business_date}*.csv"))
    #     )

    # def print_counts(self):
    #     print(f"# of BanksimLogs log files: {self.banksim_logs_files}")
    #     print(f"# of Input files in CRS: {self.input_files_crs}")
    #     print(f"# of Input files in PMA: {self.input_files_pma}")
    #     print(f"# of Input files in TBA: {self.input_files_tba}")
    #     print(f"# of Output files in CRS: {self.output_files_crs}")
    #     print(f"# of Output files in PMA: {self.output_files_pma}")
    #     print(f"# of Output files in TBA: {self.output_files_tba}")
