from TarManager import *
from FileChecker import *

import os
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd


class DatabaseManager:
    def __init__(self, files_dir, business_date):
        self.files_dir = files_dir

        datetime_obj = datetime.strptime(business_date, "%Y%m%d")
        self.business_date = datetime_obj.strftime("%Y-%m-%d")

        # Create a .env and make a variable named DB_CONN_STR that contains the login info
        conn_str = os.getenv("DB_CONN_STR")

        self.engine = create_engine(conn_str)

    def get_morning_check_table(self):
        morning_query = f"SELECT * FROM dbo.MorningCheck"

        df_morning = pd.read_sql(morning_query, self.engine)

        return df_morning
    
    def get_alert_table(self):
        alert_query = f"SELECT * FROM dbo.Metric"

        df_metric = pd.read_sql(alert_query, self.engine)

        return df_metric

    def get_trade_counts(self):

        tradequery = f"SELECT count(*) AS TBA_Trades FROM TradeBooking.Trades WHERE TradeDate='{self.business_date}'"
        tradeloans = f"SELECT count(*) AS TBA_LoanTrades FROM TradeBooking.LoanTrades WHERE TradeDate='{self.business_date}'"
        traderepo = f"SELECT count(*) AS TBA_RepoTrades FROM TradeBooking.RepoTrades WHERE TradeDate='{self.business_date}'"

        # PoseManagement Trades Queries
        pma_trades_query = f"SELECT count(*) AS PMA_Trades FROM PoseManagement.Trades WHERE TradeDate='{self.business_date}'"
        pma_loan_trades_query = f"SELECT count(*) AS PMA_LoanTrades FROM PoseManagement.LoanTrades WHERE TradeDate='{self.business_date}'"
        pma_repo_trades_query = f"SELECT count(*) AS PMA_RepoTrades FROM PoseManagement.RepoTrades WHERE TradeDate='{self.business_date}'"

        # creditriskdb BackOffice Queries
        crs_loan_query = f"SELECT count(*) AS CRS_Loan FROM creditriskdb.BackOffice_Loan WHERE Timestamp='{self.business_date}'"
        crs_repo_query = f"SELECT count(*) AS CRS_Repo FROM creditriskdb.BackOffice_Repo WHERE Timestamp='{self.business_date}'"

        # iloc gets the 0's in the first column and first row
        df_trade = pd.read_sql(tradequery, self.engine).iloc[0, 0]
        df_tradeloans = pd.read_sql(tradeloans, self.engine).iloc[0, 0]
        df_traderepo = pd.read_sql(traderepo, self.engine).iloc[0, 0]
        df_pma_trades = pd.read_sql(pma_trades_query, self.engine).iloc[0, 0]
        df_pma_loan_trades = pd.read_sql(pma_loan_trades_query, self.engine).iloc[0, 0]
        df_pma_repo_trades = pd.read_sql(pma_repo_trades_query, self.engine).iloc[0, 0]
        df_crs_loan = pd.read_sql(crs_loan_query, self.engine).iloc[0, 0]
        df_crs_repo = pd.read_sql(crs_repo_query, self.engine).iloc[0, 0]

        data = {
            "tba_trades": df_trade,
            "tba_loantrades": df_tradeloans,
            "tba_repotrades": df_traderepo,
            "pma_trades": df_pma_trades,
            "pma_loantrades": df_pma_loan_trades,
            "pma_repotrades": df_pma_repo_trades,
            "crs_loantrades": df_crs_loan,
            "crs_repotrades": df_crs_repo,
        }

        return data