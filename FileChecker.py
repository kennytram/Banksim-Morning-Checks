import os
import glob


class FileChecker:
    def __init__(self, base_dir, business_date):
        self.base_dir = base_dir
        self.business_date = business_date

        # Directories
        self.dir_banksimlogs = os.path.join(base_dir, "banksimlogs", business_date)
        self.dir_input_crs = os.path.join(base_dir, "crs", "data", "input")
        self.dir_input_pma = os.path.join(base_dir, "pma", "data", "input")
        self.dir_input_tba = os.path.join(base_dir, "tba", "data", "input")
        self.dir_output_crs = os.path.join(base_dir, "crs", "data", "output")
        self.dir_output_pma = os.path.join(base_dir, "pma", "data", "output")
        self.dir_output_tba = os.path.join(base_dir, "tba", "data", "output")

        # Count
        self.count_files()

    def count_files(self):
        # Count log files
        self.banksim_logs_files = sum(
            len(glob.glob(os.path.join(self.dir_banksimlogs, subdir, "*.log")))
            for subdir in ["crs", "pma", "tba"]
        )

        # Count input files
        self.input_files_crs = len(
            glob.glob(os.path.join(self.dir_input_crs, f"*{self.business_date}*.csv"))
        )
        self.input_files_pma = len(
            glob.glob(os.path.join(self.dir_input_pma, f"*{self.business_date}*.csv"))
        )
        self.input_files_tba = len(
            glob.glob(os.path.join(self.dir_input_tba, f"*{self.business_date}*.csv"))
        )

        # Count output files
        self.output_files_crs = len(
            glob.glob(os.path.join(self.dir_output_crs, "risk_dataset.xls"))
        )
        self.output_files_pma = len(
            glob.glob(os.path.join(self.dir_output_pma, f"*{self.business_date}*.csv"))
        )
        self.output_files_tba = len(
            glob.glob(os.path.join(self.dir_output_tba, f"*{self.business_date}*.csv"))
        )

    def print_counts(self):
        print(f"# of BanksimLogs log files: {self.banksim_logs_files}")
        print(f"# of Input files in CRS: {self.input_files_crs}")
        print(f"# of Input files in PMA: {self.input_files_pma}")
        print(f"# of Input files in TBA: {self.input_files_tba}")
        print(f"# of Output files in CRS: {self.output_files_crs}")
        print(f"# of Output files in PMA: {self.output_files_pma}")
        print(f"# of Output files in TBA: {self.output_files_tba}")
