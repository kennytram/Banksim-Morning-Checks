import FileChecker

import os
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd

class DatabaseManager:
    def __init__(self, files_dir):
        self.files_dir = files_dir

    def get_trade_counts(self, date):

        # Create a .env and make a variable named DB_CONN_STR that contains the login info
        conn_str = os.getenv("DB_CONN_STR")

        datetime_obj = datetime.strptime(date, "%Y%m%d")
        res = datetime_obj.strftime("%Y-%m-%d")
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
        df_trade = pd.read_sql(tradequery, engine).iloc[0, 0]
        df_tradeloans = pd.read_sql(tradeloans, engine).iloc[0, 0]
        df_traderepo = pd.read_sql(traderepo, engine).iloc[0, 0]
        df_pma_trades = pd.read_sql(pma_trades_query, engine).iloc[0, 0]
        df_pma_loan_trades = pd.read_sql(pma_loan_trades_query, engine).iloc[0, 0]
        df_pma_repo_trades = pd.read_sql(pma_repo_trades_query, engine).iloc[0, 0]
        df_crs_loan = pd.read_sql(crs_loan_query, engine).iloc[0, 0]
        df_crs_repo = pd.read_sql(crs_repo_query, engine).iloc[0, 0]

        print(
            df_trade,
            df_tradeloans,
            df_traderepo,
            df_pma_trades,
            df_pma_loan_trades,
            df_pma_repo_trades,
            df_crs_loan,
            df_crs_repo,
        )
        checker = FileChecker(self.files_dir, date)
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
