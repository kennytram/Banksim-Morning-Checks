from Banksim import *
from FileWriter import *
import os
import argparse
from datetime import datetime
import dotenv
import holidays


us_holidays = holidays.US()
REPORT_DIR = "/home/teamsupport2/logs"
TEAM_DIR = "/home/teamsupport2"


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
        # print(f"{business_date} is a business date.")
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
base_dir = "../blobmount" if IS_DEVELOPMENT else "/home/azureuser/blobmount"

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

        data = banksim.make_health_check_report()
        file_writer = FileWriter(data)
        file_writer.write(f"morning_report_{args.business_date}.log", REPORT_DIR)
        file_writer.write(f"latest_morning_check_report.log", TEAM_DIR)
        print(data)

        banksim.alert_check()

        banksim.db_manager.session.close()
        # # Gather Files and Count Number of Files
        # print("File Metrics : Total Input, Ouput and Log files")
        # print(
        #     "=================================================================================================="
        # )
        # banksim.tba.count_files()
        # banksim.pma.count_files()
        # banksim.crs.count_files()
        # print("tba:", banksim.tba.count_data)
        # print("pma:", banksim.pma.count_data)
        # print("crs:", banksim.crs.count_data)

        # print("\n")

        # # Calculate trades
        # print(
        #     "Reconciliation : Compare trades received ( in input ) VS trades loaded ( in DB )"
        # )
        # print(
        #     "=================================================================================================="
        # )
        # banksim.get_trade_counts()

        # print("\n")

        # # Find errors
        # print("Errors check : Include all the possible errors we can find")
        # print(
        #     "=================================================================================================="
        # )
        # banksim.tba.find_errors()
        # print(banksim.tba.error_data)

        # print("\n")

        # # Archive # works on linux
        # print("Archiving : tar.gz the logs, output and input")
        # print(
        #     "=================================================================================================="
        # )
        # banksim.tba.archive()
        # banksim.pma.archive()
        # banksim.crs.archive()
        # print("tba:", banksim.tba.archive_data)
        # print("pma:", banksim.pma.archive_data)
        # print("crs:", banksim.crs.archive_data)

        # print("\n")

        # # Find # of log files by category
        # print("Log files : Display the number of log files by category")
        # print(
        #     "=================================================================================================="
        # )
        # print(
        #     "tba's # of loadtrades:",
        #     banksim.tba.file_checker.get_num_files(
        #         banksim.tba.dirs["logs"], "*loantrades*.log"
        #     ),
        # )
        # print(
        #     "tba's # of repotrades:",
        #     banksim.tba.file_checker.get_num_files(
        #         banksim.tba.dirs["logs"], "*repotrades*.log"
        #     ),
        # )
        # print(
        #     "pma's # of loads:",
        # banksim.pma.file_checker.get_num_files(
        #     banksim.pma.dirs["logs"], "load*.log"
        # ),
        # )
        # print(
        #     "pma's # of eod_extracts:",
        #     banksim.pma.file_checker.get_num_files(
        #         banksim.pma.dirs["logs"], "eod_extract*.log"
        #     ),
        # )
        # print(
        #     "crs's # of loads:",
        #     banksim.crs.file_checker.get_num_files(
        #         banksim.crs.dirs["logs"], "load*.log"
        #     ),
        # )
        # print(
        #     "crs's # of risks:",
        #     banksim.crs.file_checker.get_num_files(
        #         banksim.crs.dirs["logs"], "risk*.log"
        #     ),
        # )

        # print("\n")

        # # Find missing files
        # print(
        #     "Reconciliation Log : if an output is generated based on the log, check in the data output folder"
        # )
        # print(
        #     "=================================================================================================="
        # )
        # print("Missing file(s) in crs:", banksim.crs.find_missing_files())

        # print("\n")

        # print("The script should not run on a weekend or a banking holiday")
        # print(
        #     "=================================================================================================="
        # )
        # print("List of Holidays:", [holiday for day, holiday in us_holidays.items()])

        # print("\n")

        # print(
        #     "Database : Build a database table to store the result of the morning check"
        # )
        # print(
        #     "=================================================================================================="
        # )
        # # banksim.db_manager.create_morning_check_table()
        # banksim.db_manager.get_morning_check_table(args.business_date)

        # print("\n")

        # print(
        #     "Build a control to retrieve the number of file having a size changing by +/- 20%"
        # )
        # print(
        #     "=================================================================================================="
        # )
        # print("pma:", banksim.pma.check_file_anomalies())
        # print("crs:", banksim.crs.check_file_anomalies())

        # print("\n")

        # print("Using the database, check the trades input between tba, pma and crs")
        # print(
        #     "=================================================================================================="
        # )
        # banksim.trade_chain_reconciliation()

        # print("\n")
        # print("Alert Table")
        # print(
        #     "=================================================================================================="
        # )
        # # banksim.alert_check()
        # print(banksim.db_manager.get_alert_table())

        # banksim.db_manager.session.close()
