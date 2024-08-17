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
import DatabaseManager

import os
import glob
import argparse
import pandas as pd
import zipfile
from sqlalchemy import create_engine
from datetime import datetime
import dotenv


# global variables that are changeable
dotenv.load_dotenv()
IS_DEVELOPMENT = os.getenv("IS_DEVELOPMENT")
SYS_NECESSARY_FILES = {"crs": {}, "pma": {}, "tba": {}}
base_dir = "./blobmount" if IS_DEVELOPMENT else "/home/azureuser/blobmount"


class Banksim:

    def __init__(self, dir: str, business_dates: list) -> None:
        self.__systems = {
            "tba": TBAHealthCheck(dir),
            "pma": PMAHealthCheck(dir),
            "crs": CRSHealthCheck(dir),
        }
        self.business_dates = business_dates
        self.db_manager = DatabaseManager()

    def tba(self) -> ApplicationHealthCheck:
        return self.__systems["tba"]

    def pma(self) -> ApplicationHealthCheck:
        return self.__systems["pma"]

    def crs(self) -> ApplicationHealthCheck:
        return self.__systems["crs"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "business_date",
        type=str,
        nargs="+",
        help="One or more business dates in YYYYMMDD format",
    )
    args = parser.parse_args()

    banksim = Banksim(args.business_date)

    checker = FileChecker(base_dir, args.business_date)

    checker.print_counts()
    dbm = DatabaseManager(base_dir)
    dbm.get_trade_counts(args.business_date)
