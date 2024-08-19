"""
# Original Checks
# File Metrics : Total Input, Ouput and Log files
# Reconciliation : Compare trades received ( in input ) VS trades loaded ( in DB )
# Errors check : Include all the possible errors we can find
# Archiving : tar.gz the logs, output and input
# Checks added Tuesday
# Log files : Display the number of log files by category (i.e load_market_data, load_trade )
    - Each System should be responsible for this
    - Can use FileChecker for this  
# Checks added Wednesday
# Reconciliation Log : For each extracting process, analyze log files and if an output is generated based on the log, check in the data output folder
# New checks
# The script should not run on a weekend or a banking holiday
# Data Storage
# Database : Build a database table to store the result of the morning check, including all the checks list above
# File Size Anomaly :
# Build a control to retrieve the number of file having a size changing by +/- 20%
# Scope : PMA/CRS application. All files
# Example : Size change between backoffice_repo20240610.csv and backoffice_repo20240611.csv
# Trade Chain Reconciliation
# Using the database, check the trades input between tba, pma and crs. Only consider all types of trades between tba and pma. Consider only repo and loan between pma and crs.
# Alerts :
# Based on the table below setup alerts for each check
"""

import FileChecker
import TarManager
from ApplicationHealthCheck import *
from DatabaseManager import *

import os
import glob
import argparse
import pandas as pd
import zipfile
from sqlalchemy import create_engine
from datetime import datetime
import dotenv
from datetime import date
import holidays

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
        self.business_date = business_date
        self.db_manager = DatabaseManager(base_dir, business_date)

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
        morning_check_status = {}
        print(data)

        if (
            data["tba_trades"] + data["tba_loantrades"] + data["tba_repotrades"]
            == self.tba.trade_data["total"]
        ):
            print("TBA are equal between input and database")
        else:
            print(self.tba.trade_data)
            print("TBA are not equal between input and database")

        if (
            data["pma_trades"] + data["pma_loantrades"] + data["pma_repotrades"]
            == self.pma.trade_data["total"]
        ):
            print("PMA are equal between input and database")
        else:
            print(self.pma.trade_data)
            print("PMA are not equal between input and database")

        if (
            data["crs_loantrades"] + data["crs_repotrades"]
            == self.crs.trade_data["total"]
        ):
            print("CRS are equal between input and database")
        else:
            print(self.crs.trade_data)
            print("CRS are not equal between input and database")

    def trade_chain_reconciliation(self) -> None:
        data = self.db_manager.get_trade_counts()
        if data["tba_trades"] != data["pma_trades"]:
            print("TBA and PMA trades are not equal")
        if data["tba_loantrades"] != data["pma_loantrades"]:
            print("TBA and PMA loan trades are not equal")
        if data["tba_repotrades"] != data["pma_repotrades"]:
            print("TBA and PMA repo trades are not equal")
        if data["pma_loantrades"] != data["crs_loantrades"]:
            print("PMA and CRS loan trades are not equal")
        if data["pma_repotrades"] != data["crs_repotrades"]:
            print("PMA and CRS repo trades are not equal")


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

        # # Gather Files
        # banksim.tba.count_files()
        # banksim.pma.count_files()
        # banksim.crs.count_files()

        # # Count Number of Files
        # print(banksim.tba.count_data)
        # print(banksim.pma.count_data)
        # print(banksim.crs.count_data)

        # # Archive # works on linux
        # banksim.tba.archive()
        # print(banksim.tba.archive_data)

        # Find errors
        # banksim.tba.find_errors()
        # print(banksim.tba.error_data)

        # # Find # of log files by category
        # print(banksim.pma.file_checker.get_num_files(banksim.pma.dirs["logs"], "load*.log"))
        # print(banksim.pma.file_checker.get_num_files(banksim.pma.dirs["logs"], "eod_extract*.log"))

        # Find missing files
        print(banksim.crs.find_missing_files())

        # Calculate trades
        # banksim.tba.
        print(banksim.tba.trade_data)
        print(banksim.pma.trade_data)
        print(banksim.crs.trade_data)

        # checker = FileChecker(base_dir, args.business_date)

        # checker.print_counts()
        banksim.get_trade_counts()
        banksim.trade_chain_reconciliation()
