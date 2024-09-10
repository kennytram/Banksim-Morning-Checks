from TarManager import *
from FileChecker import *
from datetime import datetime, timedelta
import holidays
import re

us_holidays = holidays.US()
TEAM_DIR = "/home/teamsupport2/archive"


# Brandon
def holiday_check(date):
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y%m%d").date()
    if date.weekday() >= 5:
        return True
    elif date in us_holidays:
        return True
    else:
        return False


def get_prev_date(date_str: str) -> str:
    date = datetime.strptime(date_str, "%Y%m%d").date()
    temp = date - timedelta(days=1)
    if date.weekday() == 0:
        temp = date - timedelta(days=3)
    while holiday_check(date):
        temp = temp - timedelta(days=1)
    prev_date = temp.strftime("%Y%m%d")
    return prev_date


class ApplicationHealthCheck:

    def __init__(self, base_dir, business_date):
        self.name = ""
        self.symbol = ""
        self.base_dir = base_dir
        self.business_date = business_date
        self.prev_business_date = get_prev_date(self.business_date)

        self.dirs = set()
        self.dir_file_patterns = {
            "logs": {"*.log"},
            "input": {f"*_{self.business_date}*.csv"},
            "output": {f"*_{self.business_date}*.csv"},
        }

        self.file_categories_patterns = dict()
        self.data_patterns = dict()

        self.count_data = dict()
        self.archive_data = dict()
        self.error_data = dict()
        self.missing_file_data = list()
        self.file_anomalies = list()

        self.file_checker = FileChecker(self.base_dir)
        self.tar_manager = TarManager(TEAM_DIR)

        self.files = {"logs": set(), "input": set(), "output": set()}
        self.generated_file_messages = [
            "Data successfully written to ",
            "Data written to CSV file ",
        ]

    def gather_files(self):
        for dir in self.files:
            self.files[dir] = self.file_checker.get_files(
                self.dirs[dir], self.dir_file_patterns[dir]
            )

    def set_dirs(self):
        self.dirs = {
            "logs": f"{self.base_dir}/banksimlogs/{self.business_date}/{self.symbol.lower()}",
            "input": f"{self.base_dir}/{self.symbol.lower()}/data/input",
            "output": f"{self.base_dir}/{self.symbol.lower()}/data/output",
        }

    def count_files(self) -> None:
        for dir, files in self.files.items():
            self.count_data[dir] = len(files)

    def archive(self) -> None:
        for dir, dir_path in self.dirs.items():
            tar_name = f"{self.symbol.lower()}_{dir}_{self.business_date}"
            self.archive_data[f"tar_{dir}"] = self.tar_manager.archive(
                dir_path, tar_name, self.dir_file_patterns[dir]
            )

    def find_errors(self) -> None:
        self.error_data["ERROR"] = self.file_checker.find_occurrences(
            "ERROR", self.files["logs"]
        )
        self.error_data["CRITICAL"] = self.file_checker.find_occurrences(
            "CRITICAL", self.files["logs"]
        )
        self.error_data["WARN"] = self.file_checker.find_occurrences(
            "WARN", self.files["logs"]
        )
        self.error_data["EMERGENCY"] = self.file_checker.find_occurrences(
            "EMERGENCY", self.files["logs"]
        )
        self.error_data["FATAL"] = self.file_checker.find_occurrences(
            "FATAL", self.files["logs"]
        )

    def find_missing_files(self) -> set:
        missing_files = set()

        for category, file_patterns in self.file_categories_patterns.items():
            for pattern in file_patterns:
                full_pattern = os.path.join(self.dirs[category], pattern)
                matching_files = glob.glob(full_pattern)
                if not any(file in self.files[category] for file in matching_files):
                    missing_files.add(full_pattern)

        generated_files = self.file_checker.find_phrases(
            self.files["logs"], self.generated_file_messages
        )
        for files in generated_files.values():
            for file in files:
                file_name = file.split("/")[-1]
                whole_file_name = os.path.join(self.dirs["output"], file_name)
                if whole_file_name not in self.files["output"]:
                    missing_files.add(whole_file_name)

        self.missing_file_data.extend(list(missing_files))
        return missing_files

    def find_files_no_newline(self) -> set:
        files_no_newlines = set()

        for dir, files in self.files.items():
            for file in files:
                if self.file_checker.check_newline_at_end(file):
                    files_no_newlines.add(file)

        return files_no_newlines

    def calculate_received(self) -> None:
        return

    def add_file_pattern(self, dir: str, s: str) -> None:
        self.dir_file_patterns[dir].add(s)

    def check_file_anomalies(self) -> list:

        def normalize_filename(file_name, business_date):
            base_name = file_name.replace(business_date, "")
            # Remove all digits and underlines
            normalized_name = re.sub(r"[\d_+]", "", base_name)
            return normalized_name

        def find_matching_files(
            current_files, prev_files, business_date, prev_business_date
        ):
            current_normalized = {
                normalize_filename(file, business_date): file for file in current_files
            }
            prev_normalized = {
                normalize_filename(file, prev_business_date): file
                for file in prev_files
            }
            common_normalized = set(current_normalized.keys()) & set(
                prev_normalized.keys()
            )

            matching_files = [
                (prev_normalized[name], current_normalized[name])
                for name in common_normalized
            ]
            return matching_files

        file_anomalies = []

        # Input/Output Computation
        for dir_category in ["input", "output"]:
            for file_pattern in self.file_categories_patterns[dir_category]:
                if self.business_date not in file_pattern:
                    break
                prev_file = file_pattern.replace(
                    self.business_date, self.prev_business_date
                )
                full_path_prev = os.path.join(self.dirs[dir_category], prev_file)
                if os.path.isfile(full_path_prev):
                    full_path_curr = os.path.join(self.dirs[dir_category], file_pattern)
                    if os.path.isfile(full_path_curr):
                        try:
                            prev_file_size = os.path.getsize(full_path_prev)
                            curr_file_size = os.path.getsize(full_path_curr)
                        except FileNotFoundError:
                            return []
                        if (
                            curr_file_size > prev_file_size * 1.2
                            or prev_file_size * 0.8 > curr_file_size
                        ):
                            file_anomalies.append(
                                (
                                    full_path_prev,
                                    full_path_curr,
                                    prev_file_size,
                                    curr_file_size,
                                )
                            )

        # Logs Computation
        prev_logs_dir = self.dirs["logs"].replace(
            self.business_date, self.prev_business_date
        )
        try:
            prev_logs_dir_files = os.listdir(prev_logs_dir)
            prev_logs_dir_files = [
                os.path.join(prev_logs_dir, f) for f in prev_logs_dir_files
            ]
        except FileNotFoundError:
            return file_anomalies

        list_curr_dir_files = sorted(self.files["logs"])
        list_prev_dir_files = sorted(prev_logs_dir_files)

        matching_log_files = None
        if len(prev_logs_dir_files) != len(list_curr_dir_files):
            matching_log_files = find_matching_files(
                list_curr_dir_files,
                prev_logs_dir_files,
                self.business_date,
                self.prev_business_date,
            )
        if matching_log_files:
            min_dir_files = None
            prev_files = None
            curr_files = None
            if len(prev_logs_dir_files) < len(self.files["logs"]):
                min_dir_files = sorted(prev_logs_dir_files)
                prev_files = min_dir_files
                curr_files = [curr_file for prev_file, curr_file in matching_log_files]
                curr_files.sort()
            else:
                min_dir_files = sorted(list_curr_dir_files)
                prev_files = [prev_file for prev_file, curr_file in matching_log_files]
                curr_files = min_dir_files
                prev_files.sort()
            for idx in range(len(matching_log_files)):
                try:
                    prev_file_size = os.path.getsize(prev_files[idx])
                    curr_file_size = os.path.getsize(curr_files[idx])
                except FileNotFoundError:
                    return file_anomalies
                if (
                    curr_file_size > prev_file_size * 1.2
                    or prev_file_size * 0.8 > curr_file_size
                ):
                    file_anomalies.append(
                        (
                            prev_files[idx],
                            curr_files[idx],
                            prev_file_size,
                            curr_file_size,
                        )
                    )

        else:
            for idx in range(len(list_curr_dir_files)):
                try:
                    prev_file_size = os.path.getsize(list_prev_dir_files[idx])
                    curr_file_size = os.path.getsize(list_curr_dir_files[idx])
                except FileNotFoundError:
                    return file_anomalies
                if (
                    curr_file_size > prev_file_size * 1.2
                    or prev_file_size * 0.8 > curr_file_size
                ):
                    file_anomalies.append(
                        (
                            list_prev_dir_files[idx],
                            list_curr_dir_files[idx],
                            prev_file_size,
                            curr_file_size,
                        )
                    )
        self.file_anomalies.extend(file_anomalies)
        return file_anomalies


class TBAHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir, business_date):
        super().__init__(base_dir, business_date)
        self.name = "Trade Booking App"
        self.symbol = "TBA"

        self.file_categories_patterns = {
            "logs": [
                f"eod_extract_loan_trades_*.log",
                f"eod_extract_repo_trades_*.log",
                f"monitor_and_load_client_trades_*.log",
                f"load_sod_positions_*.log",
                f"extract_eod_trades_*.log",
            ],
            "input": [
                f"positions_{self.business_date}.csv",
            ],
            "output": [
                f"eod_loan_trades_{self.business_date}.csv",
                f"eod_repo_trades_{self.business_date}.csv",
                f"eod_trades_{self.business_date}.csv",
            ],
        }

        self.set_dirs()
        self.gather_files()

        self.data_patterns = {
            "general": f"{self.dirs['input']}/*_trades_{self.business_date}_*",
            "loan": f"{self.dirs['input']}/*_loantrades_{self.business_date}_*",
            "repo": f"{self.dirs['input']}/*_repotrades_{self.business_date}_*",
        }
        self.trade_data = dict()
        self.calculate_received()

        self.position_data = 0
        self.calculate_positions()

    def calculate_received(self) -> None:
        for key, pattern in self.data_patterns.items():
            count_no_header = self.file_checker.count_csv_rows_matching_files(pattern)
            self.trade_data[key] = count_no_header
        self.trade_data["total"] = (
            self.trade_data["loan"]
            + self.trade_data["repo"]
            + self.trade_data["general"]
        )

    def calculate_positions(self) -> None:
        first_column_idx = 0
        date_obj = datetime.strptime(self.prev_business_date, "%Y%m%d")
        formatted_date = date_obj.strftime("%Y-%m-%d")

        input_dir = self.dirs["input"]
        positions_file_pattern = f"{input_dir}/positions_{self.business_date}.csv"
        self.position_data = (
            self.file_checker.count_csv_rows_matching_files_matching_columns(
                positions_file_pattern, formatted_date, first_column_idx
            )
        )


class PMAHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir, business_date):
        super().__init__(base_dir, business_date)
        self.name = "Position Management App"
        self.symbol = "PMA"

        self.file_categories_patterns = {
            "logs": [
                f"eod_extract_loan_trades_*.log",
                f"eod_extract_repo_trades_*.log",
                f"load_eod_trades_*.log",
                f"load_market_data_*.log",
                f"load_referential_data_*.log",
                f"position_computation_*.log",
                f"extract_client_position_*.log",
                f"eod_extract_positions_*.log",
            ],
            "input": [
                f"Client_PTF_{self.business_date}.csv",
                f"Clients_{self.business_date}.csv",
                f"FX_{self.business_date}.csv",
                f"eod_loan_trades_{self.business_date}.csv",
                f"eod_repo_trades_{self.business_date}.csv",
                f"eod_trades_{self.business_date}.csv",
                f"stock_data_{self.business_date}.csv",
            ],
            "output": [
                f"backoffice_loans_{self.business_date}.csv",
                f"backoffice_repo_{self.business_date}.csv",
                f"collat_data_{self.business_date}.csv",
                f"extract_client_position_{self.business_date}.csv",
                f"positions_{self.business_date}.csv",
            ],
        }

        self.set_dirs()
        self.gather_files()

        self.data_patterns = {
            "general": f"{self.dirs['input']}/eod_trades_*{self.business_date}*",
            "loan": f"{self.dirs['input']}/eod_loan_trades_*{self.business_date}*",
            "repo": f"{self.dirs['input']}/eod_repo_trades_*{self.business_date}*",
        }

        self.trade_data = dict()
        self.calculate_received()

    def calculate_received(self) -> None:
        for key, pattern in self.data_patterns.items():
            count_no_header = self.file_checker.count_csv_rows_matching_files(pattern)
            self.trade_data[key] = count_no_header
        self.trade_data["total"] = (
            self.trade_data["loan"]
            + self.trade_data["repo"]
            + self.trade_data["general"]
        )


class CRSHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir, business_date):
        super().__init__(base_dir, business_date)
        self.name = "Credit Risk System"
        self.symbol = "CRS"

        self.file_categories_patterns = {
            "logs": [
                f"load_market_data_*.log",
                f"load_referential_data_*.log",
                f"load_trades_*.log",
                f"risk_computation_*.log",
                f"risk_dataset_generation_*.log",
            ],
            "input": [
                f"Client_PTF_{self.business_date}.csv",
                f"Clients_{self.business_date}.csv",
                f"Clients_rating_{self.business_date}.csv",
                f"FX_{self.business_date}.csv",
                f"MasterContractProductData_{self.business_date}.csv",
                f"collat_data_{self.business_date}.csv",
                f"backoffice_repo_{self.business_date}.csv",
                f"backoffice_loans_{self.business_date}.csv",
                f"master_contract_{self.business_date}.csv",
                f"credit_limit_data_{self.business_date}.csv",
                f"stock_data_{self.business_date}.csv",
            ],
            "output": [],
        }

        self.add_file_pattern("output", "*.xls")
        self.add_file_pattern("output", "*.xlsx")
        self.set_dirs()
        self.gather_files()

        self.data_patterns = {
            "loan": f"{self.dirs['input']}/backoffice_loans_{self.business_date}.csv",
            "repo": f"{self.dirs['input']}/backoffice_repo_{self.business_date}.csv",
        }

        self.trade_data = dict()
        self.calculate_received()

    def calculate_received(self) -> None:
        last_column_idx = -1
        date_obj = datetime.strptime(self.business_date, "%Y%m%d")
        formatted_date = date_obj.strftime("%Y-%m-%d")

        for key, pattern in self.data_patterns.items():
            count_no_header = (
                self.file_checker.count_csv_rows_matching_files_matching_columns(
                    pattern, formatted_date, last_column_idx
                )
            )
            self.trade_data[key] = count_no_header
        self.trade_data["total"] = self.trade_data["loan"] + self.trade_data["repo"]
