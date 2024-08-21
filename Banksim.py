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
        self.tc_data = self.db_manager.get_trade_counts()
        self.trade_reconciliation_alert = defaultdict()
        self.trade_chain_reconciliation_alert = defaultdict()

        self.total_db_trade_counts = {
            "tba": 0,
            "pma": 0,
            "crs": 0,
        }

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
        print("Database:", self.tc_data)

        self.total_db_trade_counts["tba"] = (
            self.tc_data["tba_trades"]
            + self.tc_data["tba_loantrades"]
            + self.tc_data["tba_repotrades"]
        )
        if self.total_db_trade_counts["tba"] == self.tba.trade_data["total"]:
            print("TBA are equal between input and database")
        else:
            print("tba:", self.tba.trade_data)
            print("TBA are not equal between input and database")
            self.trade_reconciliation_alert["tba"] = "RED"

        self.total_db_trade_counts["pma"] = (
            self.tc_data["pma_trades"]
            + self.tc_data["pma_loantrades"]
            + self.tc_data["pma_repotrades"]
        )
        if self.total_db_trade_counts["pma"] == self.pma.trade_data["total"]:
            print("PMA are equal between input and database")
        else:
            print("pma:", self.pma.trade_data)
            print("PMA are not equal between input and database")
            self.trade_reconciliation_alert["pma"] = "RED"

        self.total_db_trade_counts["crs"] = (
            self.tc_data["crs_loantrades"] + self.tc_data["crs_repotrades"]
        )
        if self.total_db_trade_counts["crs"] == self.crs.trade_data["total"]:
            print("crs's loantrades:", self.tc_data["crs_loantrades"])
            print("CRS are equal between input and database")
        else:
            print("CRS are not equal between input and database")
            self.trade_reconciliation_alert["crs"] = "RED"

    def trade_chain_reconciliation(self) -> None:
        if self.tc_data["tba_trades"] != self.tc_data["pma_trades"]:
            print("TBA and PMA trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if self.tc_data["tba_loantrades"] != self.tc_data["pma_loantrades"]:
            print("TBA and PMA loan trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if self.tc_data["tba_repotrades"] != self.tc_data["pma_repotrades"]:
            print("TBA and PMA repo trades are not equal")
            self.trade_chain_reconciliation_alert["tba"] = "RED"
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if self.tc_data["pma_loantrades"] != self.tc_data["crs_loantrades"]:
            print("PMA and CRS loan trades are not equal")
            self.trade_chain_reconciliation_alert["pma"] = "RED"
            self.trade_chain_reconciliation_alert["crs"] = "RED"
        if self.tc_data["pma_repotrades"] != self.tc_data["crs_repotrades"]:
            print("PMA and CRS repo trades are not equal")
            self.trade_chain_reconciliation_alert["pma"] = "RED"
            self.trade_chain_reconciliation_alert["crs"] = "RED"

    def alert_check(self) -> None:
        alerts = defaultdict(dict)
        curr_count_data = defaultdict(dict)
        prev_count_data = defaultdict(dict)
        data = list()
        for system in ["pma", "crs"]:
            for dir, count in self.__systems[system].count_data.items():
                curr_count_data[system][dir] = count

        self.__prev_date_systems["tba"].count_files()
        self.__prev_date_systems["pma"].count_files()
        self.__prev_date_systems["crs"].count_files()
        for system in ["pma", "crs"]:
            for dir, count in self.__prev_date_systems[system].count_data.items():
                prev_count_data[system][dir] = count

        # 1, 2, 3 Number of Input, Logs, Output
        for system in ["pma", "crs"]:
            for i, dir in enumerate(curr_count_data[system]):
                sys_dir_check = MorningCheck(business_date=self.business_date)
                curr_count = curr_count_data[system].get(dir, 0)
                prev_count = prev_count_data[system].get(dir, 0)

                if curr_count != prev_count:
                    print(f"{system}'s {dir}: Number of files mismatched")
                    alerts[system][dir] = "RED"
                    sys_dir_check.metric_value = str(abs(curr_count - prev_count))
                    sys_dir_check.alert_triggered = 1
                sys_dir_check.app = system.lower()
                sys_dir_check.metric_id = i + 1
                data.append(sys_dir_check)

        for system in self.__systems:
            # 4 Trade Reconciliation
            sys_tr_check = MorningCheck(business_date=self.business_date)
            if self.trade_reconciliation_alert.get(system, False):
                print(f"{system}'s Trade Reconciliation Alert")
                alerts[system]["trade_reconciliation"] = "RED"
                sys_tr_check.alert_triggered = True
                diff = abs(
                    self.__systems[system].trade_data["total"]
                    - self.total_db_trade_counts[system]
                )
                sys_tr_check.metric_value = str(diff)
            sys_tr_check.app = system.lower()
            sys_tr_check.metric_id = 4
            data.append(sys_tr_check)

            # 5 Trade Chain Reconcilation
            sys_tcr_check = MorningCheck(business_date=self.business_date)
            if self.trade_chain_reconciliation_alert.get(system, False):
                print(f"{system}'s Trade Chain Reconciliation Alert")
                alerts[system]["trade_chain_reconciliation"] = "RED"
                sys_tcr_check.alert_triggered = True
            sys_tcr_check.app = system.lower()
            sys_tcr_check.metric_id = 5
            data.append(sys_tcr_check)

            # 6 Error Check
            sys_error_check = MorningCheck(business_date=self.business_date)
            if self.__systems[system].error_data.get("ERROR") or self.__systems[
                system
            ].error_data.get("CRITICAL"):
                print(f"{system}'s Error Check Alert")
                alerts[system]["error"] = "RED"
                sys_error_check.alert_triggered = True
                errors_length = len(self.__systems[system].error_data.get("ERROR"))
                criticals_length = len(
                    self.__systems[system].error_data.get("CRITICAL")
                )
                sys_error_check.metric_value = str(errors_length + criticals_length)
            sys_error_check.app = system.lower()
            sys_error_check.metric_id = 6
            data.append(sys_error_check)

            # 7 Reconciliation Log - Output Files
            sys_rec_check = MorningCheck(business_date=self.business_date)
            if self.__systems[system].missing_file_data:
                print(f"{system}'s Missing File(s) Alert")
                alerts[system]["missing"] = "RED"
                sys_rec_check.alert_triggered = True
                sys_rec_check.metric_value = str(
                    len(self.__systems[system].missing_file_data)
                )
            sys_rec_check.app = system.lower()
            sys_rec_check.metric_id = 7
            data.append(sys_rec_check)

            # 8 File Size Anomaly
            if system != "tba":
                sys_fsa_check = MorningCheck(business_date=self.business_date)
                if self.__systems[system].file_anomalies:
                    print(f"{system}'s File Size Anomalies Alert")
                    alerts[system]["file_anomalies"] = "RED"
                    sys_fsa_check.alert_triggered = True
                    sys_fsa_check.metric_value = str(
                        len(self.__systems[system].file_anomalies)
                    )
                sys_fsa_check.app = system.lower()
                sys_fsa_check.metric_id = 8
                data.append(sys_fsa_check)

        self.db_manager.insert_morning_check(data)
