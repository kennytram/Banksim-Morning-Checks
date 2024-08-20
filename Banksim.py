from ApplicationHealthCheck import *
from DatabaseManager import *

from collections import defaultdict


class Banksim:

    def __init__(self, dir: str, business_date: str) -> None:
        self.__systems = {
            "tba": TBAHealthCheck(dir, business_date),
            "pma": PMAHealthCheck(dir, business_date),
            "crs": CRSHealthCheck(dir, business_date),
        }
        prev_date = get_prev_date(business_date)
        self.__prev_date_systems = {
            "tba": TBAHealthCheck(dir, prev_date),
            "pma": PMAHealthCheck(dir, prev_date),
            "crs": CRSHealthCheck(dir, prev_date),
        }
        self.business_date = business_date
        self.db_manager = DatabaseManager(dir, business_date)
        self.trade_reconciliation_alert = defaultdict()
        self.trade_chain_reconciliation_alert = defaultdict()

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

        print("Database:", data)

        if (
            data["tba_trades"] + data["tba_loantrades"] + data["tba_repotrades"]
            == self.tba.trade_data["total"]
        ):
            print("TBA are equal between input and database")
        else:
            print("tba:", self.tba.trade_data)
            print("TBA are not equal between input and database")
            self.trade_reconciliation_alert["tba"] = "RED"

        if (
            data["pma_trades"] + data["pma_loantrades"] + data["pma_repotrades"]
            == self.pma.trade_data["total"]
        ):
            print("PMA are equal between input and database")
        else:
            print("pma:", self.pma.trade_data)
            print("PMA are not equal between input and database")
            self.trade_reconciliation_alert["pma"] = "RED"

        if (
            data["crs_loantrades"] + data["crs_repotrades"]
            == self.crs.trade_data["total"]
        ):
            print("CRS are equal between input and database")
        else:
            print("crs:", self.crs.trade_data)
            print("CRS are not equal between input and database")
            self.trade_reconciliation_alert["crs"] = "RED"

    def trade_chain_reconciliation(self) -> None:
        data = self.db_manager.get_trade_counts()
        if data["tba_trades"] != data["pma_trades"]:
            print("TBA and PMA trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if data["tba_loantrades"] != data["pma_loantrades"]:
            print("TBA and PMA loan trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if data["tba_repotrades"] != data["pma_repotrades"]:
            print("TBA and PMA repo trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if data["pma_loantrades"] != data["crs_loantrades"]:
            print("PMA and CRS loan trades are not equal")
            self.trade_chain_reconciliation_alert["pma"] = "RED"
            self.trade_chain_reconciliation_alert["crs"] = "RED"
        if data["pma_repotrades"] != data["crs_repotrades"]:
            print("PMA and CRS repo trades are not equal")
            self.trade_chain_reconciliation_alert["pma"] = "RED"
            self.trade_chain_reconciliation_alert["crs"] = "RED"

    def alert(self) -> None:
        alerts = defaultdict(dict)
        curr_count_data = defaultdict(dict)
        prev_count_data = defaultdict(dict)

        for system in ["pma", "crs"]:
            for dir, count in self.__systems[system].count_data.items():
                curr_count_data[system][dir] = count

        for systen in ["pma", "crs"]:
            for dir, count in self.__prev_date_systems[systen].count_data.items():
                prev_count_data[systen][dir] = count

        # 1, 2, 3 Number of Input, Logs, Output
        for system in ["pma", "crs"]:
            for dir in curr_count_data[system]:
                curr_count = curr_count_data[system].get(dir, 0)
                prev_count = prev_count_data[system].get(dir, 0)

                if curr_count != prev_count:
                    print(f"{system}'s {dir}: Number of files mismatched")
                    alerts[system][dir] = "RED"

        for system in self.__systems:
            # 4 Trade Reconciliation
            if self.trade_reconciliation_alert.get(system, False):
                print(f"{system}'s Trade Reconciliation Alert")
                alerts[system]["trade_reconciliation"] = "RED"

            # 5 Trade Chain Reconcilation
            if self.trade_chain_reconciliation_alert.get(system, False):
                print(f"{system}'s Trade Chain Reconciliation Alert")
                alerts[system]["trade_chain_reconciliation"] = "RED"

            # 6 Error Check
            if self.__systems[system].error_data.get("ERROR") or self.__systems[
                system
            ].error_data.get("CRITICAL"):
                print(f"{system}'s Error Check Alert")
                alerts[system]["error"] = "RED"

            # 7 Reconciliation Log - Output Files
            if self.__systems[system].missing_file_data:
                print(f"{system}'s Missing File(s) Alert")
                alerts[system]["missing"] = "RED"

            # 8 File Size Anomaly
            if self.__systems[system].file_anomalies:
                print(f"{system}'s File Size Anomalies Alert")
                alerts[system]["file_anomalies"] = "RED"
