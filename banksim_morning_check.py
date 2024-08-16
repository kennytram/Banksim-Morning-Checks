"""import pandas as pd
import pyodbc

conn_str = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=banksim.database.windows.net;'
    r'DATABASE=opscodb;'
    r'UID=banksimdb02;' 
    r'PWD=CAVABIENALLEZLA!4;' 
)



def get_trade_counts(date):

    conn = pyodbc.connect(conn_str)
    tradequery = f"SELECT count(*) AS TBA_Trades FROM TradeBooking.Trades WHERE TradeDate='{date}'"
    tradeloans = f"SELECT count(*) AS TBA_LoanTrades FROM TradeBooking.LoanTrades WHERE TradeDate='{date}'"
    traderepo = f"SELECT count(*) AS TBA_RepoTrades FROM TradeBooking.RepoTrades WHERE TradeDate='{date}'"
    pma_trades_query = f"SELECT count(*) AS PMA_Trades FROM PoseManagement.Trades WHERE TradeDate='{date}'"
    pma_loan_trades_query = f"SELECT count(*) AS PMA_LoanTrades FROM PoseManagement.LoanTrades WHERE TradeDate='{date}'"
    pma_repo_trades_query = f"SELECT count(*) AS PMA_RepoTrades FROM PoseManagement.RepoTrades WHERE TradeDate='{date}'"

    # creditriskdb BackOffice Queries
    crs_loan_query = f"SELECT count(*) AS CRS_Loan FROM creditriskdb.BackOffice_Loan WHERE Timestamp='{date}'"
    crs_repo_query = f"SELECT count(*) AS CRS_Repo FROM creditriskdb.BackOffice_Repo WHERE Timestamp='{date}'"

    # Execute the queries
    df_pma_trades = pd.read_sql(pma_trades_query, conn)
    df_pma_loan_trades = pd.read_sql(pma_loan_trades_query, conn)
    df_pma_repo_trades = pd.read_sql(pma_repo_trades_query, conn)
    df_crs_loan = pd.read_sql(crs_loan_query, conn)
    df_crs_repo = pd.read_sql(crs_repo_query, conn)


    df_trade = pd.read_sql(tradequery, conn)
    df_tradeloans = pd.read_sql(tradeloans, conn)
    df_traderepo = pd.read_sql(traderepo, conn)

    conn.close()

    print(df_trade, df_tradeloans, df_traderepo, df_pma_trades, df_pma_loan_trades, df_pma_repo_trades, df_crs_loan, df_crs_repo)
    
get_trade_counts('2024-06-14')"""

import pandas as pd
from sqlalchemy import create_engine

conn_str = (
    "mssql+pyodbc://banksimdb02:CAVABIENALLEZLA!4@banksim.database.windows.net/opscodb"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

def get_trade_counts(date):
    engine = create_engine(conn_str)

    tradequery = f"SELECT count(*) AS TBA_Trades FROM TradeBooking.Trades WHERE TradeDate='{date}'"
    tradeloans = f"SELECT count(*) AS TBA_LoanTrades FROM TradeBooking.LoanTrades WHERE TradeDate='{date}'"
    traderepo = f"SELECT count(*) AS TBA_RepoTrades FROM TradeBooking.RepoTrades WHERE TradeDate='{date}'"

    # PoseManagement Trades Queries
    pma_trades_query = f"SELECT count(*) AS PMA_Trades FROM PoseManagement.Trades WHERE TradeDate='{date}'"
    pma_loan_trades_query = f"SELECT count(*) AS PMA_LoanTrades FROM PoseManagement.LoanTrades WHERE TradeDate='{date}'"
    pma_repo_trades_query = f"SELECT count(*) AS PMA_RepoTrades FROM PoseManagement.RepoTrades WHERE TradeDate='{date}'"

    # creditriskdb BackOffice Queries
    crs_loan_query = f"SELECT count(*) AS CRS_Loan FROM creditriskdb.BackOffice_Loan WHERE Timestamp='{date}'"
    crs_repo_query = f"SELECT count(*) AS CRS_Repo FROM creditriskdb.BackOffice_Repo WHERE Timestamp='{date}'"

    # Execute the queries
    df_trade = pd.read_sql(tradequery, engine)
    df_tradeloans = pd.read_sql(tradeloans, engine)
    df_traderepo = pd.read_sql(traderepo, engine)
    df_pma_trades = pd.read_sql(pma_trades_query, engine)
    df_pma_loan_trades = pd.read_sql(pma_loan_trades_query, engine)
    df_pma_repo_trades = pd.read_sql(pma_repo_trades_query, engine)
    df_crs_loan = pd.read_sql(crs_loan_query, engine)
    df_crs_repo = pd.read_sql(crs_repo_query, engine)

    print(df_trade, df_tradeloans, df_traderepo, df_pma_trades, df_pma_loan_trades, df_pma_repo_trades, df_crs_loan, df_crs_repo)
    
get_trade_counts('2024-06-14')
import os
import glob
import argparse

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

    base_dir = 'C:\\Users\\Tony\\OPSCO Training\\team_support_doc\\blobmount'

    checker = FileChecker(base_dir, args.business_date)
    
    checker.print_counts()