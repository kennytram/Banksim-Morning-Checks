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
mc_conn_str = os.getenv("MC_DB_CONN_STR")

engine = create_engine(conn_str)
mc_engine = create_engine(mc_conn_str)
session = Session(engine)
mc_session = Session(mc_engine)
meta = MetaData()

Base = declarative_base()


class Alert(Base):
    __tablename__ = "Metric"
    __table_args__ = {"schema": "team02"}
    metric_id = Column("metricid", Integer, primary_key=True, nullable=False)
    metric_name = Column("metricname", String(255), nullable=False)
    alert_rule = Column("alertrule", String(255))
    scope = Column("scope", String(100))

    def __repr__(self):
        return f"<Metric(metricid='{self.metric_id}', metricname='{self.metric_name}', alert_rule='{self.alert_rule}', scope='{self.scope}')>"


class MorningCheck(Base):
    __tablename__ = "MorningCheck"
    __table_args__ = {"schema": "team02"}
    id = Column("id", Integer, primary_key=True, autoincrement=True)
    business_date = Column("businessdate", Date, nullable=False)
    app = Column("app", String(100), nullable=False)
    metric_id = Column(
        "metricid",
        Integer,
        ForeignKey("team02.Metric.metricid"),
        nullable=False,
        # "metricid", Integer, ForeignKey("Metric.metricid"), nullable=False
    )
    metric_value = Column("metricvalue", String(100), default="N/A")
    alert_triggered = Column("alerttriggered", Boolean, default=False, nullable=False)
    date_added = Column("dateadded", Date, nullable=False)
    date_modified = Column("datemodified", Date, nullable=False)

    def __repr__(self):
        return f"<MorningCheck(id='{self.id}', businessdate='{self.business_date}', app='{self.app}, metricid='{self.metric_id}', metricvalue='{self.metric_value}', alerttriggered='{self.alert_triggered}', 'dateadded='{self.date_added}', datemodified='{self.date_modified}'>"


class DatabaseManager:
    def __init__(self, files_dir, business_date):
        self.files_dir = files_dir

        datetime_obj = datetime.strptime(business_date, "%Y%m%d")
        self.business_date = datetime_obj.strftime("%Y-%m-%d")
        self.session = session
        self.mc_session = mc_session

    def get_morning_check_table(self, date):
        datetime_obj = datetime.strptime(date, "%Y%m%d")
        date = datetime_obj.strftime("%Y-%m-%d")

        try:
            morning_check_table = Table("MorningCheck", meta, autoload_with=mc_engine)
            query = select(MorningCheck).where(MorningCheck.business_date == date)
            # return pd.read_sql_table(table_name="MorningCheck", con=engine)
            with mc_engine.connect() as conn:
                for row in conn.execute(query):
                    print(row)
        except NoSuchTableError:
            print("MorningCheck table does not exist in db")
        except Exception as e:
            print(f"Error: {e}")

    def create_morning_check_table(self):
        try:
            meta.create_all(mc_engine, tables=[MorningCheck.__table__])
        except NoReferencedTableError:
            print("Alert table needs to be created first")
        except Exception as e:
            print(f"Error: {e}")

    def insert_morning_check(self, data):
        try:
            for check in data:
                existing_record = (
                    mc_session.query(MorningCheck)
                    .filter_by(
                        business_date=check.business_date,
                        app=check.app,
                        metric_id=check.metric_id,
                    )
                    .first()
                )

                if existing_record:
                    existing_record.metric_value = (
                        check.metric_value if check.metric_value is not None else "N/A"
                    )
                    existing_record.alert_triggered = check.alert_triggered or False
                    existing_record.date_modified = datetime.now()

                else:
                    mc_session.add(
                        MorningCheck(
                            business_date=check.business_date,
                            app=check.app,
                            metric_id=check.metric_id,
                            metric_value=(
                                check.metric_value
                                if check.metric_value is not None
                                else "N/A"
                            ),
                            alert_triggered=(check.alert_triggered or False),
                            date_added=datetime.now(),
                            date_modified=datetime.now(),
                        )
                    )
                mc_session.commit()
        except Exception as e:
            self.mc_session.rollback()
            print(f"Error: {e}")

    def get_alert_table(self):
        try:
            alert_table = Table("Metric", meta, autoload_with=mc_engine)
            return pd.read_sql_table(table_name="Metric", con=mc_engine)
        except NoSuchTableError:
            print("Metric table does not exist in db")
        except Exception as e:
            print(f"Error: {e}")

    def create_alert_table(self):
        try:
            meta.create_all(mc_engine, tables=[Alert.__table__])
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
            mc_session.add_all(metrics)
            mc_session.commit()
        except Exception as e:
            mc_session.rollback()
            print(f"Error: {e}")
        return

    def get_duplicate_data(self):
        # TradeBooking Trade Queries
        tradequery = text(
            "SELECT count(clienttradeid), clienttradeid, tradedate, clientcode FROM TradeBooking.Trades GROUP BY clienttradeid, tradedate, clientcode HAVING COUNT(clienttradeid) >= 2"
        )
        tradeloans = text(
            "SELECT count(clienttradeid), clienttradeid, tradedate, clientcode FROM TradeBooking.LoanTrades GROUP BY clienttradeid, tradedate, clientcode HAVING COUNT(clienttradeid) >= 2"
        )
        traderepo = text(
            "SELECT count(clienttradeid), clienttradeid, tradedate, clientcode FROM TradeBooking.RepoTrades GROUP BY clienttradeid, tradedate, clientcode HAVING COUNT(clienttradeid) >= 2"
        )

        # PoseManagement Trades Queries
        pma_trades_query = text(
            "SELECT count(clienttradeid), clienttradeid, tradedate, clientcode FROM PoseManagement.Trades GROUP BY clienttradeid, tradedate, clientcode HAVING COUNT(clienttradeid) >= 2"
        )
        pma_loan_trades_query = text(
            "SELECT count(clienttradeid), clienttradeid, tradedate, clientcode FROM PoseManagement.LoanTrades GROUP BY clienttradeid, tradedate, clientcode HAVING COUNT(clienttradeid) >= 2"
        )
        pma_repo_trades_query = text(
            "SELECT count(clienttradeid), clienttradeid, tradedate, clientcode FROM PoseManagement.RepoTrades GROUP BY clienttradeid, tradedate, clientcode HAVING COUNT(clienttradeid) >= 2"
        )

        # creditriskdb BackOffice Queries
        crs_loan_query = text(
            f"SELECT count(tradeid), tradeid, timestamp, clientid FROM creditriskdb.BackOffice_Loan WHERE timestamp = '{self.business_date}' GROUP BY tradeid, timestamp, clientid HAVING COUNT(tradeid) >= 2"
        )
        crs_repo_query = text(
            f"SELECT count(tradeid), tradeid, timestamp, clientid FROM creditriskdb.BackOffice_Repo WHERE timestamp = '{self.business_date}' GROUP BY tradeid, timestamp, clientid HAVING COUNT(tradeid) >= 2"
        )

        trade_results = session.execute(tradequery).fetchall()
        tradeloans_results = session.execute(tradeloans).fetchall()
        traderepo_results = session.execute(traderepo).fetchall()

        pma_trade_results = session.execute(pma_trades_query).fetchall()
        pma_tradeloans_results = session.execute(pma_loan_trades_query).fetchall()
        pma_traderepo_results = session.execute(pma_repo_trades_query).fetchall()

        crs_tradeloans_results = session.execute(crs_loan_query).fetchall()
        crs_traderepo_results = session.execute(crs_repo_query).fetchall()

        trade_list = [str(row.clienttradeid) for row in trade_results]
        tradeloans_list = [str(row.clienttradeid) for row in tradeloans_results]
        traderepo_list = [str(row.clienttradeid) for row in traderepo_results]

        pma_trade_list = [str(row.clienttradeid) for row in pma_trade_results]
        pma_tradeloans_list = [str(row.clienttradeid) for row in pma_tradeloans_results]
        pma_traderepo_list = [str(row.clienttradeid) for row in pma_traderepo_results]

        crs_tradeloans_list = [str(row.tradeid) for row in crs_tradeloans_results]
        crs_traderepo_list = [str(row.tradeid) for row in crs_traderepo_results]

        data = {
            "tba_trades": trade_list,
            "tba_loantrades": tradeloans_list,
            "tba_repotrades": traderepo_list,
            "pma_trades": pma_trade_list,
            "pma_loantrades": pma_tradeloans_list,
            "pma_repotrades": pma_traderepo_list,
            "crs_loantrades": crs_tradeloans_list,
            "crs_repotrades": crs_traderepo_list,
        }

        return data

    def get_trade_counts(self):
        # TradeBooking Trade Queries
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

    def get_position_counts(self, date) -> None:
        tba_positions_query = f"SELECT count(*) AS TBA_Positions FROM TradeBooking.Positions WHERE BusinessDate='{date}'"
        df_tba_positions = pd.read_sql(tba_positions_query, engine).iloc[0, 0]
        return df_tba_positions
