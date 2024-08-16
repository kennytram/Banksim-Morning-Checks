import os
import glob
import argparse
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


#global variables that are changeable
base_dir = 'C:\\ProductPractice\\Week 3\\Banksim-Morning-Checks\\blobmount'
parser = argparse.ArgumentParser()
parser.add_argument("business_date")
args = parser.parse_args()

class FileChecker:
    def __init__(self, base_dir, business_date):
        self.base_dir = base_dir
        self.business_date = business_date
        
        # Directories
        self.dir_banksimlogs = os.path.join(base_dir, 'banksimlogs', business_date)
        self.dir_input_crs = os.path.join(base_dir, 'crs', 'data', 'input')
        self.dir_input_pma = os.path.join(base_dir, 'pma', 'data', 'input')
        self.dir_input_tba = os.path.join(base_dir, 'tba', 'data', 'input')
        self.dir_output_pma = os.path.join(base_dir, 'pma', 'data', 'output')
        self.dir_output_tba = os.path.join(base_dir, 'tba', 'data', 'output')

        # Count
        self.count_files()

    def count_files(self):
        # Count log files
        self.banksim_logs_files = sum(len(glob.glob(os.path.join(self.dir_banksimlogs, subdir, '*.log')))
                                      for subdir in ['crs', 'pma', 'tba'])

        # Count input files
        self.input_files_crs = len(glob.glob(os.path.join(self.dir_input_crs, f'*{self.business_date}*.csv')))
        self.input_files_pma = len(glob.glob(os.path.join(self.dir_input_pma, f'*{self.business_date}*.csv')))
        self.input_files_tba = len(glob.glob(os.path.join(self.dir_input_tba, f'*{self.business_date}*.csv')))
        
        # Count output files
        self.output_files_pma = len(glob.glob(os.path.join(self.dir_output_pma, f'*{self.business_date}*.csv')))
        self.output_files_tba = len(glob.glob(os.path.join(self.dir_output_tba, f'*{self.business_date}*.csv')))

    def print_counts(self):
        print(f"# of BanksimLogs log files: {self.banksim_logs_files}")
        print(f"# of Input files in CRS: {self.input_files_crs}")
        print(f"# of Input files in PMA: {self.input_files_pma}")
        print(f"# of Input files in TBA: {self.input_files_tba}")
        print(f"# of Output files in PMA: {self.output_files_pma}")
        print(f"# of Output files in TBA: {self.output_files_tba}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("business_date")
    args = parser.parse_args()
#C:\ProductPractice\Week 3\Banksim-Morning-Checks\blobmount
    base_dir = 'C:\\ProductPractice\\Week 3\\Banksim-Morning-Checks\\blobmount'

    checker = FileChecker(base_dir, args.business_date)
    
    checker.print_counts()

conn_str = (
    "mssql+pyodbc://banksimdb02:CAVABIENALLEZLA!4@banksim.database.windows.net/opscodb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

def get_trade_counts(date):
    
    datetime_obj = datetime.strptime(date, '%Y%m%d')
    res = datetime_obj.strftime('%Y-%m-%d')
    engine = create_engine(conn_str)

    tradequery = f"SELECT count(*) AS TBA_Trades FROM TradeBooking.Trades WHERE TradeDate='{res}'"
    tradeloans = f"SELECT count(*) AS TBA_LoanTrades FROM TradeBooking.LoanTrades WHERE TradeDate='{res}'"
    traderepo = f"SELECT count(*) AS TBA_RepoTrades FROM TradeBooking.RepoTrades WHERE TradeDate='{res}'"

    # PoseManagement Trades Queries
    pma_trades_query = f"SELECT count(*) AS PMA_Trades FROM PoseManagement.Trades WHERE TradeDate='{res}'"
    pma_loan_trades_query = f"SELECT count(*) AS PMA_LoanTrades FROM PoseManagement.LoanTrades WHERE TradeDate='{res}'"
    pma_repo_trades_query = f"SELECT count(*) AS PMA_RepoTrades FROM PoseManagement.RepoTrades WHERE TradeDate='{res}'"

    # creditriskdb BackOffice Queries
    crs_loan_query = f"SELECT count(*) AS CRS_Loan FROM creditriskdb.BackOffice_Loan WHERE Timestamp='{res}'"
    crs_repo_query = f"SELECT count(*) AS CRS_Repo FROM creditriskdb.BackOffice_Repo WHERE Timestamp='{res}'"

    # iloc gets the 0's in the first column and first row
    df_trade = pd.read_sql(tradequery, engine).iloc[0,0]
    df_tradeloans = pd.read_sql(tradeloans, engine).iloc[0,0]
    df_traderepo = pd.read_sql(traderepo, engine).iloc[0,0]
    df_pma_trades = pd.read_sql(pma_trades_query, engine).iloc[0,0]
    df_pma_loan_trades = pd.read_sql(pma_loan_trades_query, engine).iloc[0,0]
    df_pma_repo_trades = pd.read_sql(pma_repo_trades_query, engine).iloc[0,0]
    df_crs_loan = pd.read_sql(crs_loan_query, engine).iloc[0,0]
    df_crs_repo = pd.read_sql(crs_repo_query, engine).iloc[0,0]

    print(df_trade, df_tradeloans, df_traderepo, df_pma_trades, df_pma_loan_trades, df_pma_repo_trades, df_crs_loan, df_crs_repo)
    checker = FileChecker(base_dir, date)
    if df_trade + df_tradeloans + df_traderepo == checker.banksim_logs_files:
        print("TBA are equal")
    else:
        print("red alert")
    if df_pma_loan_trades + df_pma_repo_trades == checker.input_files_pma:
        
        print("PMA are equal")
    else:
        print(checker.input_files_pma)
        print("red alert")
    if df_tradeloans + df_traderepo == checker.input_files_crs:
        print("CRS are equal")

    else:
        print(checker.input_files_crs)
        print("red alert")

get_trade_counts('20240614')

#checker = FileChecker(base_dir, args)



