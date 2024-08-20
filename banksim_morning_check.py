from ApplicationHealthCheck import *
from DatabaseManager import *

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import dotenv
from datetime import date
import holidays
from collections import defaultdict

us_holidays = holidays.US()


# Brandon
def holiday_check(business_date):
    if isinstance(business_date, str):
        business_date = datetime.strptime(business_date, "%Y%m%d").date()
    if business_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        print(f"{business_date} is a weekend.")
        return True  # Return to indicate it's a weekend
    elif business_date in us_holidays:
        print(f"{business_date} is a holiday: {us_holidays[business_date]}")
        return True  # Return to indicate it's a holiday
    else:
        print(f"{business_date} is a business date.")
        return False  # Return to indicate it's not a holiday


def get_prev_date(date_str: str) -> str:
    date = datetime.strptime(date_str, "%Y%m%d").date()
    temp = date - timedelta(days=1)
    if date.weekday() == 0:
        temp = date - timedelta(days=3)
    while holiday_check(date):
        temp = temp - timedelta(days=1)
    prev_date = temp.strftime("%Y%m%d")
    return prev_date


# global variables that are changeable
dotenv.load_dotenv()
IS_DEVELOPMENT = os.getenv("IS_DEVELOPMENT")
SYS_NECESSARY_FILES = {"crs": {}, "pma": {}, "tba": {}}
base_dir = "./blobmount" if IS_DEVELOPMENT else "/home/azureuser/blobmount"


class Banksim:

    def __init__(self, dir: str, business_date: str) -> None:
        self.__systems = {
            "tba": TBAHealthCheck(dir, business_date),
            "pma": PMAHealthCheck(dir, business_date),
            "crs": CRSHealthCheck(dir, business_date),
        }
        prev_date = get_prev_date(business_date)
        self.__prev_date_systems = {
            "tba": TBAHealthCheck(dir, prev_date),
            "pma": PMAHealthCheck(dir, prev_date),
            "crs": CRSHealthCheck(dir, prev_date),
        }
        self.business_date = business_date
        self.db_manager = DatabaseManager(base_dir, business_date)
        self.trade_reconciliation_alert = defaultdict()
        self.trade_chain_reconciliation_alert = defaultdict()

    @property
    def tba(self) -> ApplicationHealthCheck:
        return self.__systems["tba"]

    @property
    def pma(self) -> ApplicationHealthCheck:
        return self.__systems["pma"]

    @property
    def crs(self) -> ApplicationHealthCheck:
        return self.__systems["crs"]

    def get_trade_counts(self) -> None:
        data = self.db_manager.get_trade_counts()

        print("Database:", data)

        if (
            data["tba_trades"] + data["tba_loantrades"] + data["tba_repotrades"]
            == self.tba.trade_data["total"]
        ):
            print("TBA are equal between input and database")
        else:
            print("tba:", self.tba.trade_data)
            print("TBA are not equal between input and database")
            self.trade_reconciliation_alert["tba"] = "RED"

        if (
            data["pma_trades"] + data["pma_loantrades"] + data["pma_repotrades"]
            == self.pma.trade_data["total"]
        ):
            print("PMA are equal between input and database")
        else:
            print("pma:", self.pma.trade_data)
            print("PMA are not equal between input and database")
            self.trade_reconciliation_alert["pma"] = "RED"

        if (
            data["crs_loantrades"] + data["crs_repotrades"]
            == self.crs.trade_data["total"]
        ):
            print("CRS are equal between input and database")
        else:
            print("crs:", self.crs.trade_data)
            print("CRS are not equal between input and database")
            self.trade_reconciliation_alert["crs"] = "RED"

    def trade_chain_reconciliation(self) -> None:
        data = self.db_manager.get_trade_counts()
        if data["tba_trades"] != data["pma_trades"]:
            print("TBA and PMA trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if data["tba_loantrades"] != data["pma_loantrades"]:
            print("TBA and PMA loan trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if data["tba_repotrades"] != data["pma_repotrades"]:
            print("TBA and PMA repo trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if data["pma_loantrades"] != data["crs_loantrades"]:
            print("PMA and CRS loan trades are not equal")
            self.trade_chain_reconciliation_alert["pma"] = "RED"
            self.trade_chain_reconciliation_alert["crs"] = "RED"
        if data["pma_repotrades"] != data["crs_repotrades"]:
            print("PMA and CRS repo trades are not equal")
            self.trade_chain_reconciliation_alert["pma"] = "RED"
            self.trade_chain_reconciliation_alert["crs"] = "RED"

    def alert(self) -> None:
        alerts = defaultdict(dict)
        curr_count_data = defaultdict(dict)
        prev_count_data = defaultdict(dict)

        for system in ["pma", "crs"]:
            for dir, count in self.__systems[system].count_data.items():
                curr_count_data[system][dir] = count

        for systen in ["pma", "crs"]:
            for dir, count in self.__prev_date_systems[systen].count_data.items():
                prev_count_data[systen][dir] = count

        # 1, 2, 3 Number of Input, Logs, Output
        for system in ["pma", "crs"]:
            for dir in curr_count_data[system]:
                curr_count = curr_count_data[system].get(dir, 0)
                prev_count = prev_count_data[system].get(dir, 0)

                if curr_count != prev_count:
                    print(f"{system}'s {dir}: Number of files mismatched")
                    alerts[system][dir] = "RED"

        for system in self.__systems:
            # 4 Trade Reconciliation
            if self.trade_reconciliation_alert.get(system, False):
                print(f"{system}'s Trade Reconciliation Alert")
                alerts[system]["trade_reconciliation"] = "RED"

            # 5 Trade Chain Reconcilation
            if self.trade_chain_reconciliation_alert.get(system, False):
                print(f"{system}'s Trade Chain Reconciliation Alert")
                alerts[system]["trade_chain_reconciliation"] = "RED"

            # 6 Error Check
            if self.__systems[system].error_data.get("ERROR") or self.__systems[
                system
            ].error_data.get("CRITICAL"):
                print(f"{system}'s Error Check Alert")
                alerts[system]["error"] = "RED"

            # 7 Reconciliation Log - Output Files
            if self.__systems[system].missing_file_data:
                print(f"{system}'s Missing File(s) Alert")
                alerts[system]["missing"] = "RED"

            # 8 File Size Anomaly
            if self.__systems[system].file_anomalies:
                print(f"{system}'s File Size Anomalies Alert")
                alerts[system]["file_anomalies"] = "RED"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "business_date",
        type=str,
        help="Business date in YYYYMMDD format",
    )
    args = parser.parse_args()
    if not holiday_check(args.business_date):
        banksim = Banksim(base_dir, args.business_date)

        print("\n")

        # Gather Files and Count Number of Files
        print("File Metrics : Total Input, Ouput and Log files")
        print(
            "==================================================================================="
        )
        banksim.tba.count_files()
        banksim.pma.count_files()
        banksim.crs.count_files()
        print("tba:", banksim.tba.count_data)
        print("pma:", banksim.pma.count_data)
        print("crs:", banksim.crs.count_data)

        print("\n")

        # Calculate trades
        print(
            "Reconciliation : Compare trades received ( in input ) VS trades loaded ( in DB )"
        )
        print(
            "=================================================================================================="
        )
        banksim.get_trade_counts()

        print("\n")

        # Find errors
        print("Errors check : Include all the possible errors we can find")
        print(
            "=================================================================================================="
        )
        banksim.tba.find_errors()
        print(banksim.tba.error_data)

        print("\n")

        # Archive # works on linux
        print("Archiving : tar.gz the logs, output and input")
        print(
            "=================================================================================================="
        )
        banksim.tba.archive()
        print(banksim.tba.archive_data)

        print("\n")

        # Find # of log files by category
        print("Log files : Display the number of log files by category")
        print(
            "=================================================================================================="
        )
        print(
            "tba's # of loadtrades:",
            banksim.tba.file_checker.get_num_files(
                banksim.tba.dirs["logs"], "*loantrades*.log"
            ),
        )
        print(
            "tba's # of repotrades:",
            banksim.tba.file_checker.get_num_files(
                banksim.tba.dirs["logs"], "*repotrades*.log"
            ),
        )
        print(
            "pma's # of loads:",
            banksim.pma.file_checker.get_num_files(
                banksim.pma.dirs["logs"], "load*.log"
            ),
        )
        print(
            "pma's # of eod_extracts:",
            banksim.pma.file_checker.get_num_files(
                banksim.pma.dirs["logs"], "eod_extract*.log"
            ),
        )
        print(
            "crs's # of loads:",
            banksim.crs.file_checker.get_num_files(
                banksim.crs.dirs["logs"], "load*.log"
            ),
        )
        print(
            "crs's # of risks:",
            banksim.crs.file_checker.get_num_files(
                banksim.crs.dirs["logs"], "risk*.log"
            ),
        )

        print("\n")

        # Find missing files
        print(
            "Reconciliation Log : if an output is generated based on the log, check in the data output folder"
        )
        print(
            "=================================================================================================="
        )
        print("Missing file(s) in crs:", banksim.crs.find_missing_files())

        print("\n")

        print("The script should not run on a weekend or a banking holiday")
        print(
            "=================================================================================================="
        )
        print("List of Holidays:", [holiday for day, holiday in us_holidays.items()])

        print("\n")

        print(
            "Database : Build a database table to store the result of the morning check"
        )
        print(
            "=================================================================================================="
        )
        print(banksim.db_manager.get_morning_check_table())

        print("\n")

        print(
            "Build a control to retrieve the number of file having a size changing by +/- 20%"
        )
        print(
            "=================================================================================================="
        )
        print("pma:", banksim.pma.check_file_anomalies())
        print("crs:", banksim.crs.check_file_anomalies())

        print("\n")

        print("Using the database, check the trades input between tba, pma and crs")
        print(
            "=================================================================================================="
        )
        # banksim.trade_chain_reconciliation()

        print("\n")
        print("Alert Table")
        print(
            "=================================================================================================="
        )
        print(banksim.db_manager.get_alert_table())
