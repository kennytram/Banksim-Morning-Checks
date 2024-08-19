from TarManager import *
from FileChecker import *

TEAM_DIR = "/home/teamsupport2"


class ApplicationHealthCheck:

    def __init__(self, base_dir, business_date):
        self.name = ""
        self.symbol = ""
        self.base_dir = base_dir
        self.business_date = business_date

        self.dirs = set()
        self.dir_file_patterns = {
            "logs": {"*.log"},
            "input": {f"*_{self.business_date}*.csv"},
            "output": {f"*_{self.business_date}*.csv"},
        }

        self.file_categories_patterns = dict()

        self.count_data = dict()
        self.archive_data = dict()
        self.error_data = dict()
        self.missing_file_data = list()

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

    def find_missing_files(self) -> set:
        missing_files = set()

        for category, file_patterns in self.file_categories_patterns.items():
            for pattern in file_patterns:
                full_pattern = os.path.join(self.dirs[category], pattern)
                matching_files = glob.glob(full_pattern)
                if not all(file in self.files[category] for file in matching_files):
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

        return missing_files

    def add_file_pattern(self, dir: str, str: str) -> None:
        self.dir_file_patterns[dir].add(str)


class TBAHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir, business_date):
        super().__init__(base_dir, business_date)
        self.name = "Trade Booking App"
        self.symbol = "TBA"

        self.file_categories_patterns = {
            "logs": [
                f"eod_extract_loan_trades_{self.business_date}_*.log",
                f"eod_extract_repo_trades_{self.business_date}_*.log",
                f"monitor_and_load_client_trades_{self.business_date}_*.log",
            ],
            "input": [],
            "output": [
                f"eod_loan_trades_{self.business_date}.csv",
                f"eod_repo_trades_{self.business_date}.csv",
            ],
        }

        self.set_dirs()
        self.gather_files()


class PMAHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir, business_date):
        super().__init__(base_dir, business_date)
        self.name = "Position Management App"
        self.symbol = "PMA"

        self.file_categories_patterns = {
            "logs": [
                f"eod_extract_loan_trades_{self.business_date}_*.log",
                f"eod_extract_repo_trades_{self.business_date}_*.log",
                f"load_eod_trades_{self.business_date}_*.log",
                f"load_market_data_{self.business_date}_*.log",
                f"load_referential_data_{self.business_date}_*.log",
            ],
            "input": [
                f"Client_PTF_{self.business_date}.csv",
                f"Clients_{self.business_date}.csv",
                f"FX_{self.business_date}.csv",
                f"eod_loan_trades_{self.business_date}.csv",
                f"eod_repo_trades_{self.business_date}.csv",
                f"stock_data_{self.business_date}.csv",
            ],
            "output": [
                f"backoffice_loans_{self.business_date}.csv",
                f"backoffice_repo_{self.business_date}.csv",
                f"collat_data_{self.business_date}.csv",
            ],
        }

        self.set_dirs()
        self.gather_files()


class CRSHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir, business_date):
        super().__init__(base_dir, business_date)
        self.name = "Credit Risk System"
        self.symbol = "CRS"

        self.file_categories_patterns = {
            "logs": [
                f"load_market_data_{self.business_date}_*.log",
                f"load_referential_data_{self.business_date}_*.log",
                f"load_trades_{self.business_date}_*.log",
                f"risk_computation_{self.business_date}_*.log",
                f"risk_dataset_generation_{self.business_date}.log",
            ],
            "input": [
                f"Client_PTF_{self.business_date}.csv",
                f"Clients_rating_{self.business_date}.csv",
                f"MasterContractProductData_{self.business_date}.csv",
                f"backoffice_repo_{self.business_date}.csv",
                f"credit_limit_data_{self.business_date}.csv",
                f"stock_data_{self.business_date}.csv",
            ],
            "output": [],
        }

        self.add_file_pattern("output", "*.xls")
        self.set_dirs()
        self.gather_files()
