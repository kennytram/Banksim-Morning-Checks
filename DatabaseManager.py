from TarManager import *
from FileChecker import *

import os
from sqlalchemy import *
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoSuchTableError, NoReferencedTableError
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import pandas as pd
import dotenv

# Create a .env and make a variable named DB_CONN_STR that contains the login info
dotenv.load_dotenv()
conn_str = os.getenv("DB_CONN_STR")

engine = create_engine(conn_str)
session = Session(engine)
meta = MetaData()

Base = declarative_base()


class Alert(Base):
    __tablename__ = "Metric"
    metric_id = Column("metricid", Integer, primary_key=True, nullable=False)
    metric_name = Column("metricname", String(255), nullable=False)
    alert_rule = Column("alertrule", String(255))
    scope = Column("scope", String(100))

    def __repr__(self):
        return f"<Metric(metricid='{self.metric_id}', metricname='{self.metric_name}', alert_rule='{self.alert_rule}', scope='{self.scope}')>"


class MorningCheck(Base):
    __tablename__ = "MorningCheck"
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    business_date = Column("businessdate", Date, nullable=False)
    app = Column("app", String(100), nullable=False)
    metric_id = Column("metricid", Integer, ForeignKey("Metric.metricid"))
    metric_value = Column("metricvalue", Integer)
    alert_triggered = Column("alerttriggered", Boolean, nullable=False)

    def __repr__(self):
        return f"<MorningCheck(id='{self.id}', businessdate='{self.business_date}', app='{self.app}, metricid='{self.metric_id}', metricvalue='{self.metric_value}', alerttriggered='{self.alert_triggered}'>"


class DatabaseManager:
    def __init__(self, files_dir, business_date):
        self.files_dir = files_dir

        datetime_obj = datetime.strptime(business_date, "%Y%m%d")
        self.business_date = datetime_obj.strftime("%Y-%m-%d")

    def get_morning_check_table(self):
        # morning_query = f"SELECT * FROM dbo.MorningCheck"

        # df_morning = pd.read_sql(morning_query, self.engine)

        # return df_morning

        try:
            morning_check_table = Table("MorningCheck", meta, autoload_with=engine)
            morning_check_table.select()
            return pd.read_sql_table(table_name="MorningCheck", con=engine)
        except NoSuchTableError:
            print("MorningCheck table does not exist in db")
        except Exception as e:
            print(f"Error: {e}")

    def create_morning_check_table(self):
        try:
            # morning_check_table = Table(
            #     "MorningCheck",
            #     meta,
            #     Column("id", Integer, primary_key=True, autoincrement=True),
            #     Column("businessdate", Date, nullable=False),
            #     Column("app", String(100), nullable=False),
            #     Column("metricid", Integer, ForeignKey("Metric.metricid")),
            #     Column("metricvalue", Integer),
            #     Column("alerttriggered", Boolean, nullable=False)
            # )
            # meta.create_all(engine)
            meta.create_all(engine, tables=[MorningCheck.__table__])
        except NoReferencedTableError:
            print("Alert table needs to be created first")
        except Exception as e:
            print(f"Error: {e}")

    def insert_morning_check(self, data):
        return

    def get_alert_table(self):
        # alert_query = f"SELECT * FROM dbo.Metric"

        # df_metric = pd.read_sql(alert_query, self.engine)

        # return df_metric

        try:
            alert_table = Table("Metric", meta, autoload_with=engine)
            alert_table.select()
            return pd.read_sql_table(table_name="Metric", con=engine)
        except NoSuchTableError:
            print("Metric table does not exist in db")
        except Exception as e:
            print(f"Error: {e}")

    def create_alert_table(self):
        try:
            # alert_table = Table(
            #         "Metric",
            #         self.meta,
            #         Column("metricid", Integer, primary_key=True, nullable=False),
            #         Column("metricname", String(255), nullable=False),
            #         Column("alertrule", String(255)),
            #         Column("scope", String(100))
            # )
            # meta.create_all(engine)
            meta.create_all(engine, tables=[Alert.__table__])
        except Exception as e:
            print(f"Error: {e}")

    def insert_default_alert_rules(self):
        try:
            metrics = [
                Alert(
                    metric_id=1,
                    metric_name="Number of InputFiles",
                    alert_rule="if nb != previousday",
                    scope="pma,crs",
                ),
                Alert(
                    metric_id=2,
                    metric_name="Number of Logs",
                    alert_rule="if nb != previousday",
                    scope="pma,crs",
                ),
                Alert(
                    metric_id=3,
                    metric_name="Number of OutputFiles",
                    alert_rule="if nb != previousday",
                    scope="pma,crs",
                ),
                Alert(
                    metric_id=4,
                    metric_name="Trade Reconciliation",
                    alert_rule="if reconciliation fail",
                    scope="tba,pma,crs",
                ),
                Alert(
                    metric_id=5,
                    metric_name="Trade Chain Reconcilation",
                    alert_rule="if reconciliation fail",
                    scope="global",
                ),
                Alert(
                    metric_id=6,
                    metric_name="Error Check",
                    alert_rule="if > 0",
                    scope="tba,pma,crs",
                ),
                Alert(
                    metric_id=7,
                    metric_name="Reconciliation log - output files",
                    alert_rule="if reconciliation fail",
                    scope="tba,pma,crs",
                ),
                Alert(
                    metric_id=8,
                    metric_name="File Size Anomaly",
                    alert_rule="if > 0",
                    scope="pma,crs",
                ),
            ]
            for metric in metrics:
                session.add(metric)
            session.commit()
        except Exception as e:
            print(f"Error: {e}")
        return

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
        df_trade = pd.read_sql(tradequery, engine).iloc[0, 0]
        df_tradeloans = pd.read_sql(tradeloans, engine).iloc[0, 0]
        df_traderepo = pd.read_sql(traderepo, engine).iloc[0, 0]
        df_pma_trades = pd.read_sql(pma_trades_query, engine).iloc[0, 0]
        df_pma_loan_trades = pd.read_sql(pma_loan_trades_query, engine).iloc[0, 0]
        df_pma_repo_trades = pd.read_sql(pma_repo_trades_query, engine).iloc[0, 0]
        df_crs_loan = pd.read_sql(crs_loan_query, engine).iloc[0, 0]
        df_crs_repo = pd.read_sql(crs_repo_query, engine).iloc[0, 0]

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
