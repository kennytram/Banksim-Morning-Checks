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
