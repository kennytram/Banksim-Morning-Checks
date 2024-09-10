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
        self.position_data = self.db_manager.get_position_counts(prev_date)
        self.duplicate_data = self.db_manager.get_duplicate_data()
        self.trade_reconciliation_alert = defaultdict()
        self.trade_chain_reconciliation_alert = defaultdict()
        self.alerts = defaultdict(dict)

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
        self.trade_reconciliation_alert["tba"] = "GREEN"
        self.trade_reconciliation_alert["pma"] = "GREEN"
        self.trade_reconciliation_alert["crs"] = "GREEN"

        self.total_db_trade_counts["tba"] = (
            self.tc_data["tba_trades"]
            + self.tc_data["tba_loantrades"]
            + self.tc_data["tba_repotrades"]
        )
        if self.total_db_trade_counts["tba"] != self.tba.trade_data["total"]:
            self.trade_reconciliation_alert["tba"] = "RED"

        self.total_db_trade_counts["pma"] = (
            self.tc_data["pma_trades"]
            + self.tc_data["pma_loantrades"]
            + self.tc_data["pma_repotrades"]
        )
        if self.total_db_trade_counts["pma"] != self.pma.trade_data["total"]:
            self.trade_reconciliation_alert["pma"] = "RED"

        self.total_db_trade_counts["crs"] = (
            self.tc_data["crs_loantrades"] + self.tc_data["crs_repotrades"]
        )
        if self.total_db_trade_counts["crs"] != self.crs.trade_data["total"]:
            self.trade_reconciliation_alert["crs"] = "RED"

    def trade_chain_reconciliation(self) -> None:

        self.trade_chain_reconciliation_alert["tba"] = "GREEN"
        self.trade_chain_reconciliation_alert["pma"] = "GREEN"
        self.trade_chain_reconciliation_alert["crs"] = "GREEN"

        if self.tc_data["tba_trades"] != self.tc_data["pma_trades"]:
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if self.tc_data["tba_loantrades"] != self.tc_data["pma_loantrades"]:
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if self.tc_data["tba_repotrades"] != self.tc_data["pma_repotrades"]:
            self.trade_chain_reconciliation_alert["pma"] = "RED"
        if self.tc_data["pma_loantrades"] != self.tc_data["crs_loantrades"]:
            self.trade_chain_reconciliation_alert["crs"] = "RED"
        if self.tc_data["pma_repotrades"] != self.tc_data["crs_repotrades"]:
            self.trade_chain_reconciliation_alert["crs"] = "RED"

    def run(self) -> None:
        self.tba.count_files()
        self.pma.count_files()
        self.crs.count_files()
        self.get_trade_counts()
        self.trade_chain_reconciliation()
        self.tba.find_errors()
        self.pma.find_errors()
        self.crs.find_errors()
        self.tba.find_missing_files()
        self.pma.find_missing_files()
        self.crs.find_missing_files()
        self.pma.check_file_anomalies()
        self.crs.check_file_anomalies()
        self.tba.archive()
        self.pma.archive()
        self.crs.archive()

    def alert_check(self) -> None:
        alerts = self.alerts
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
                    alerts[system][dir] = "AMBER"
                    sys_dir_check.metric_value = str(abs(curr_count - prev_count))
                    sys_dir_check.alert_triggered = 1
                sys_dir_check.app = system.lower()
                if dir == 'logs':
                    sys_dir_check.metric_id = 2
                elif dir == 'input':
                    sys_dir_check.metric_id = 1
                else:
                    sys_dir_check.metric_id = 3
                data.append(sys_dir_check)

        for system in self.__systems:
            # 4 Trade Reconciliation
            sys_tr_check = MorningCheck(business_date=self.business_date)
            if self.trade_reconciliation_alert[system] != "GREEN":
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
            if self.trade_chain_reconciliation_alert[system] != "GREEN":
                alerts[system]["trade_chain_reconciliation"] = "RED"
                sys_tcr_check.alert_triggered = True
            sys_tcr_check.app = system.lower()
            sys_tcr_check.metric_id = 5
            if system == 'pma':
                sys_tcr_check.metric_value = int(self.total_db_trade_counts['tba'] - self.total_db_trade_counts['pma'])
            elif system == 'crs':
                sys_tcr_check.metric_value = int(self.total_db_trade_counts['pma'] - self.total_db_trade_counts['crs'])
            data.append(sys_tcr_check)

            # 6 Error Check
            sys_error_check = MorningCheck(business_date=self.business_date)
            if self.__systems[system].error_data.get("ERROR") or self.__systems[
                system
            ].error_data.get("CRITICAL"):
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
                    alerts[system]["file_anomalies"] = "AMBER"
                    sys_fsa_check.alert_triggered = True
                    sys_fsa_check.metric_value = str(
                        len(self.__systems[system].file_anomalies)
                    )
                sys_fsa_check.app = system.lower()
                sys_fsa_check.metric_id = 8
                data.append(sys_fsa_check)
            
        self.db_manager.insert_morning_check(data)

    def make_email_data(self) -> str:
        formatted_date = datetime.strptime(self.business_date, "%Y%m%d").strftime("%Y-%m-%d")
        # Red if errors or any trades not loading, missing_file, tr/tcr
        # Amber if archive, count not matching db, file anomalies
        tba_color_code = ""
        if any(is_error for error_code, is_error in self.tba.error_data.items()) \
            or self.trade_reconciliation_alert["tba"] == "RED" or \
                self.trade_chain_reconciliation_alert["tba"] == "RED" or self.tba.missing_file_data:
            tba_color_code = "RED"
        elif any(res is False for dir, res in self.tba.archive_data.items())  \
            or any(color == 'AMBER' for dir, color in self.alerts['tba'].items()):
            tba_color_code = "AMBER"
        else:
            tba_color_code = "GREEN"

        pma_color_code = ""
        if any(is_error for error_code, is_error in self.pma.error_data.items()) \
            or self.trade_reconciliation_alert["pma"] == "RED" or \
                self.trade_chain_reconciliation_alert["pma"] == "RED" or self.pma.missing_file_data:
            pma_color_code = "RED"
        elif any(res is False for dir, res in self.pma.archive_data.items()) or self.pma.file_anomalies \
            or any(color == 'AMBER' for dir, color in self.alerts['pma'].items()):
            pma_color_code = "AMBER"
        else:
            pma_color_code = "GREEN"

        crs_color_code = ""
        if any(is_error for error_code, is_error in self.crs.error_data.items()) \
            or self.trade_reconciliation_alert["crs"] == "RED" or \
                self.trade_chain_reconciliation_alert["crs"] == "RED" or self.crs.missing_file_data:
            crs_color_code = "RED"
        elif any(res is False for dir, res in self.crs.archive_data.items()) or self.crs.file_anomalies \
            or any(color == 'AMBER' for dir, color in self.alerts['crs'].items()):
            crs_color_code = "AMBER"
        else:
            crs_color_code = "GREEN"

        
        tba_error_count = len(self.tba.error_data["ERROR"]) + len(self.tba.error_data["CRITICAL"])
        pma_error_count = len(self.pma.error_data["ERROR"]) + len(self.pma.error_data["CRITICAL"])
        crs_error_count = len(self.crs.error_data["ERROR"]) + len(self.crs.error_data["CRITICAL"])
        
        data = []
        data.append('<br/>')
        data[-1] += f"<p class='MsoNormal'>Trade Booking System (TBA): {tba_color_code}</p>"
        data[-1] += f"Log files <strong>({self.tba.count_data['logs']}</strong> files total):"
        data[-1] += "<ul style='margin-top: 0cm;'>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Generaltrades: <strong>{self.tba.trade_data['general']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Repotrades: <strong>{self.tba.trade_data['repo']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Loantrades: <strong>{self.tba.trade_data['loan']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades log: <strong>{self.tba.file_checker.get_num_files(self.tba.dirs['logs'], f'*_trades_{self.business_date}_1_*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Repotrades log: <strong>{self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*repotrades*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Loantrades log: <strong>{self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*loantrades*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Extracts: <strong>{self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*extract*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Monitor and load: <strong>{self.tba.file_checker.get_num_files(self.tba.dirs['logs'], 'monitor_and_load*.log')}</strong> file</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Load_sod_positions: <strong>{self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*load_sod_positions*.log')}</strong> file</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades Loaded in DB: <strong>{self.total_db_trade_counts['tba']}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades received in Input: <strong>{self.tba.trade_data['total']}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Positions Loaded in DB: <strong>{self.position_data}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Positions received in Input: <strong>{self.tba.position_data}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Errors in log files: <strong>{tba_error_count}</strong></li>"

        if tba_error_count:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if self.tba.error_data["ERROR"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.tba.error_data["ERROR"].items() for error in errors])
            if self.tba.error_data["CRITICAL"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.tba.error_data["CRITICAL"].items() for error in errors])
            if self.tba.error_data["WARN"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.tba.error_data["WARN"].items() for error in errors])
            if self.tba.error_data["EMERGENCY"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.tba.error_data["EMERGENCY"].items() for error in errors])
            if self.tba.error_data["FATAL"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.tba.error_data["FATAL"].items() for error in errors])
            data[-1] += '</ul>'


        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Missing files: <strong>{len(self.tba.missing_file_data) if self.tba.missing_file_data else 'None'}</strong></li>"
        if self.tba.missing_file_data:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}</li>" for file in self.tba.missing_file_data])
            data[-1] += '</ul>'

        diff_trades = self.tba.trade_data['general'] - self.tc_data["tba_trades"]
        diff_loans = self.tba.trade_data['loan'] - self.tc_data["tba_loantrades"]
        diff_repos = self.tba.trade_data['repo'] - self.tc_data["tba_repotrades"]
        tba_tr_status = 'Bad' if self.tba.trade_data['total'] != self.total_db_trade_counts['tba'] or diff_trades or diff_loans or diff_repos else 'Good'
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trade Reconciliation: <strong>{tba_tr_status}</strong>"

        if tba_tr_status == 'Bad':
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if diff_trades:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differences in General: <strong>{abs(diff_trades)}</strong></li>"
            if diff_loans:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differeces in Loans: <strong>{abs(diff_loans)}</strong></li>"
            if diff_repos:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differeces in Repos: <strong>{abs(diff_repos)}</strong></li>"
            data[-1] += '</ul>'

        is_tba_duplicate = True if self.duplicate_data['tba_trades'] or self.duplicate_data['tba_loantrades'] or self.duplicate_data['tba_repotrades'] else False
        
        if is_tba_duplicate:
            data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Duplicate data found in:</li>"
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if self.duplicate_data['tba_trades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>TradeBooking.Trades</li>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['tba_trades']])

            if self.duplicate_data['tba_loantrades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>TradeBooking.LoanTrades</li>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['tba_loantrades']])

            if self.duplicate_data['tba_repotrades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>TradeBooking.RepoTrades</li>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['tba_repotrades']])

        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Zip Logs/Input/Output files: <strong>{'Done' if all(self.tba.archive_data.values()) else 'Fail'}</strong></li>"
        data[-1] += '</ul>'

        data.append(f"<p class='MsoNormal'>Position Management System (PMA): {pma_color_code}</p>")
        data[-1] += f"Log files <strong>({self.pma.count_data['logs']}</strong> files total):"
        data[-1] += "<ul style='margin-top: 0cm;'>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Generaltrades: <strong>{self.pma.trade_data['general']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Repotrades: <strong>{self.pma.trade_data['repo']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Loantrades: <strong>{self.pma.trade_data['loan']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Loads: <strong>{self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'load*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Eod_extracts: <strong>{self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'eod_extract*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Extract_client_position: <strong>{self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'extract_client_position*.log')}</strong> file</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Position_computation: <strong>{self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'position_computation*.log')}</strong> file</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades Loaded in DB: <strong>{self.total_db_trade_counts['pma']}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades received in Input: <strong>{self.pma.trade_data['total']}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Errors in log files: <strong>{pma_error_count}</strong></li>"

        if pma_error_count:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if self.pma.error_data["ERROR"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.pma.error_data["ERROR"].items() for error in errors])
            if self.pma.error_data["CRITICAL"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.pma.error_data["CRITICAL"].items() for error in errors])
            if self.pma.error_data["WARN"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.pma.error_data["WARN"].items() for error in errors])
            if self.pma.error_data["EMERGENCY"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.pma.error_data["EMERGENCY"].items() for error in errors])
            if self.pma.error_data["FATAL"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.pma.error_data["FATAL"].items() for error in errors])
            data[-1] += '</ul>'


        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Missing files: <strong>{len(self.pma.missing_file_data) if self.pma.missing_file_data else 'None'}</strong></li>"
        if self.pma.missing_file_data:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}</li>" for file in self.pma.missing_file_data])
            data[-1] += '</ul>'

        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>File-Anomalies by +/- 20%: <strong>{len(self.pma.file_anomalies) if self.pma.file_anomalies else 'None'}</strong></li>"
        if self.pma.file_anomalies:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>[{size1} bytes : {size2} bytes] {file1} : {file2}</li>" for file1, file2, size1, size2 in self.pma.file_anomalies])
            data[-1] += '</ul>'
        
        diff_trades = self.pma.trade_data['general'] - self.tc_data["pma_trades"]
        diff_loans = self.pma.trade_data['loan'] - self.tc_data["pma_loantrades"]
        diff_repos = self.pma.trade_data['repo'] - self.tc_data["pma_repotrades"]
        pma_tr_status = 'Bad' if self.pma.trade_data['total'] != self.total_db_trade_counts['pma'] or diff_trades or diff_loans or diff_repos else 'Good'
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trade Reconciliation: <strong>{pma_tr_status}</strong>"

        if pma_tr_status:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if diff_trades:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differences in General: <strong>{abs(diff_trades)}</strong></li>"
            if diff_loans:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differeces in Loans: <strong>{abs(diff_loans)}</strong></li>"
            if diff_repos:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differeces in Repos: <strong>{abs(diff_repos)}</strong></li>"
            data[-1] += '</ul>'

        pma_tcr_status = 'Bad' if self.trade_chain_reconciliation_alert['pma'] == 'RED' else 'Good'
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trade Chain Reconciliation between TBA and PMA: <strong>{pma_tcr_status}</strong>"
        
        if pma_tcr_status == 'Bad':
            diff = int(self.total_db_trade_counts['tba'] - self.total_db_trade_counts['pma'])
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differences between the two systems: <strong>{abs(diff_trades)}</strong></li>"
            data[-1] += "</ul>"

        is_pma_duplicate = True if self.duplicate_data['pma_trades'] or self.duplicate_data['pma_loantrades'] or self.duplicate_data['pma_repotrades'] else False
        
        if is_pma_duplicate:
            data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Duplicate data found in:</li>"
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if self.duplicate_data['pma_trades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>PoseManagement.Trades</li>"
                data[-1] += "<ul style='margin-top: 0cm;'>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['pma_trades']])
                data[-1] += "</ul>"

            if self.duplicate_data['pma_loantrades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>PoseManagement.LoanTrades</li>"
                data[-1] += "<ul style='margin-top: 0cm;'>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['pma_loantrades']])
                data[-1] += "</ul>"

            if self.duplicate_data['pma_repotrades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>PoseManagement.RepoTrades</li>"
                data[-1] += "<ul style='margin-top: 0cm;'>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['pma_repotrades']])
                data[-1] += "</ul>"
            data[-1] += "</ul>"

        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Zip Logs/Input/Output files: <strong>{'Done' if all(self.pma.archive_data.values()) else 'Fail'}</strong></li>"
        data[-1] += '</ul>'

        data.append(f"<p class='MsoNormal'>Credit Risk System (CRS): {crs_color_code}</p>")
        data[-1] += f"Log files <strong>({self.tba.count_data['logs']}</strong> files total):"
        data[-1] += "<ul style='margin-top: 0cm;'>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Repotrades: <strong>{self.tba.trade_data['repo']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Loantrades: <strong>{self.tba.trade_data['loan']}</strong> input trades</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Loads: <strong>{self.crs.file_checker.get_num_files(self.crs.dirs['logs'], 'load*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Risks: <strong>{self.crs.file_checker.get_num_files(self.crs.dirs['logs'], 'risk*.log')}</strong> files</li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades Loaded in DB: <strong>{self.total_db_trade_counts['crs']}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trades received in Input: <strong>{self.crs.trade_data['total']}</strong></li>"
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Errors in log files: <strong>{crs_error_count}</strong></li>"

        if crs_error_count:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if self.crs.error_data["ERROR"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.crs.error_data["ERROR"].items() for error in errors])
            if self.crs.error_data["CRITICAL"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.crs.error_data["CRITICAL"].items() for error in errors])
            if self.crs.error_data["WARN"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.crs.error_data["WARN"].items() for error in errors])
            if self.crs.error_data["EMERGENCY"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.crs.error_data["EMERGENCY"].items() for error in errors])
            if self.crs.error_data["FATAL"]:
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}: {error}</li>" for file, errors in self.crs.error_data["FATAL"].items() for error in errors])
            data[-1] += '</ul>'


        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Missing files: <strong>{len(self.crs.missing_file_data) if self.crs.missing_file_data else 'None'}</strong></li>"
        if self.crs.missing_file_data:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{file}</li>" for file in self.crs.missing_file_data])
            data[-1] += '</ul>'

        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>File-Anomalies by +/- 20%: <strong>{len(self.crs.file_anomalies) if self.crs.file_anomalies else 'None'}</strong></li>"
        if self.crs.file_anomalies:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>[{size1} bytes : {size2} bytes] {file1} : {file2}</li>" for file1, file2, size1, size2 in self.crs.file_anomalies])
            data[-1] += '</ul>'

        diff_loans = self.crs.trade_data['loan'] - self.tc_data["crs_loantrades"]
        diff_repos = self.crs.trade_data['repo'] - self.tc_data["crs_repotrades"]
        crs_tr_status = 'Bad' if self.crs.trade_data['total'] != self.total_db_trade_counts['crs'] or diff_loans or diff_repos else 'Good'
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trade Reconciliation: <strong>{crs_tr_status}</strong>"

        if crs_tr_status:
            data[-1] += "<ul style='margin-top: 0cm;'>"
            if diff_trades:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differences in General: <strong>{abs(diff_trades)}</strong></li>"
            if diff_loans:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differeces in Loans: <strong>{abs(diff_loans)}</strong></li>"
            if diff_repos:
                data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differeces in Repos: <strong>{abs(diff_repos)}</strong></li>"
            data[-1] += '</ul>'

        crs_tcr_status = 'Bad' if self.trade_chain_reconciliation_alert['crs'] == 'RED' else 'Good'
        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Trade Chain Reconciliation between PMA and CRS: <strong>{crs_tcr_status}</strong>"
        
        if crs_tcr_status == 'Bad':
            diff = int(self.total_db_trade_counts['pma'] - self.total_db_trade_counts['crs'])
            data[-1] += "<ul style='margin-top: 0cm;'>"
            data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Differences between the two systems: <strong>{abs(diff_trades)}</strong></li>"
            data[-1] += "</ul>"

        is_crs_duplicate = True if self.duplicate_data['crs_loantrades'] or self.duplicate_data['crs_repotrades'] else False
        
        if is_crs_duplicate:
            data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Duplicate data found in:</li>"
            data[-1] += "<ul style='margin-top: 0cm;'>"

            if self.duplicate_data['crs_loantrades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>creditriskdb.BackOffice_Loan</li>"
                data[-1] += "<ul style='margin-top: 0cm;'>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['crs_loantrades']])
                data[-1] += "</ul>"

            if self.duplicate_data['crs_repotrades']:
                data[-1] += "<li class='MsoListParagraph' style='margin-left: 0cm;'>creditriskdb.BackOffice_Loan_Repo</li>"
                data[-1] += "<ul style='margin-top: 0cm;'>"
                data[-1] += ''.join([f"<li class='MsoListParagraph' style='margin-left: 0cm;'>{trade_id}</li>" for trade_id in self.duplicate_data['crs_repotrades']])
                data[-1] += "</ul>"
            data[-1] += "</ul>"

        data[-1] += f"<li class='MsoListParagraph' style='margin-left: 0cm;'>Zip Logs/Input/Output files: <strong>{'Done' if all(self.crs.archive_data.values()) else 'Fail'}</strong></li>"
        data[-1] += '</ul>'
        
        return ''.join(data)

    def make_health_check_report(self) -> str:
        formatted_date = datetime.strptime(self.business_date, "%Y%m%d").strftime("%Y-%m-%d")
        # Red if errors or any trades not loading, missing_file, tr/tcr
        # Amber if archive, count not matching db, file anomalies
        tba_color_code = ""
        if any(is_error for error_code, is_error in self.tba.error_data.items()) \
            or self.trade_reconciliation_alert["tba"] == "RED" or \
                self.trade_chain_reconciliation_alert["tba"] == "RED" or self.tba.missing_file_data:
            tba_color_code = "RED"
        elif any(res is False for dir, res in self.tba.archive_data.items())  \
            or any(color == 'AMBER' for dir, color in self.alerts['tba'].items()):
            tba_color_code = "AMBER"
        else:
            tba_color_code = "GREEN"

        pma_color_code = ""
        if any(is_error for error_code, is_error in self.pma.error_data.items()) \
            or self.trade_reconciliation_alert["pma"] == "RED" or \
                self.trade_chain_reconciliation_alert["pma"] == "RED" or self.pma.missing_file_data:
            pma_color_code = "RED"
        elif any(res is False for dir, res in self.pma.archive_data.items()) or self.pma.file_anomalies \
            or any(color == 'AMBER' for dir, color in self.alerts['pma'].items()):
            pma_color_code = "AMBER"
        else:
            pma_color_code = "GREEN"

        crs_color_code = ""
        if any(is_error for error_code, is_error in self.crs.error_data.items()) \
            or self.trade_reconciliation_alert["crs"] == "RED" or \
                self.trade_chain_reconciliation_alert["crs"] == "RED" or self.crs.missing_file_data:
            crs_color_code = "RED"
        elif any(res is False for dir, res in self.crs.archive_data.items()) or self.crs.file_anomalies \
            or any(color == 'AMBER' for dir, color in self.alerts['crs'].items()):
            crs_color_code = "AMBER"
        else:
            crs_color_code = "GREEN"

        
        tba_error_count = len(self.tba.error_data["ERROR"]) + len(self.tba.error_data["CRITICAL"])
        pma_error_count = len(self.pma.error_data["ERROR"]) + len(self.pma.error_data["CRITICAL"])
        crs_error_count = len(self.crs.error_data["ERROR"]) + len(self.crs.error_data["CRITICAL"])

        tba_files_no_newline = self.tba.find_files_no_newline()
        pma_files_no_newline = self.pma.find_files_no_newline()
        crs_files_no_newline = self.crs.find_files_no_newline()
        
        data = []
        data.append(f"Morning Check Report {formatted_date}")
        data.append("-----------------------------------------------------------------------")
        data.append("=======================================================================")
        data.append(f"Trade Booking System (TBA): {tba_color_code}")
        data.append("=======================================================================")
        data.append(f"Log files ({self.tba.count_data['logs']} files total):")
        data.append(f"Generaltrades: {self.tba.trade_data['general']} input trades")
        data.append(f"Repotrades: {self.tba.trade_data['repo']} input trades")
        data.append(f"Loantrades: {self.tba.trade_data['loan']} input trades")
        data.append(f"Trades log: {self.tba.file_checker.get_num_files(self.tba.dirs['logs'], f'*_trades_{self.business_date}_1_*.log')} files")
        data.append(f"Repotrades log: {self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*repotrades*.log')} files")
        data.append(f"Loantrades log: {self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*loantrades*.log')} files")
        data.append(f"Extracts: {self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*extract*.log')} files")
        data.append(f"Monitor and load: {self.tba.file_checker.get_num_files(self.tba.dirs['logs'], 'monitor_and_load*.log')} file")
        data.append(f"Load_sod_positions: {self.tba.file_checker.get_num_files(self.tba.dirs['logs'], '*load_sod_positions*.log')} file")
        data.append(f"Trades Loaded in DB: {self.total_db_trade_counts['tba']}")
        data.append(f"Trades received in Input: {self.tba.trade_data['total']}")
        data.append(f"Positions Loaded in DB: {self.position_data}")
        data.append(f"Positions received in Input: {self.tba.position_data}")
        data.append(f"Errors in log files: {tba_error_count}")

        if tba_error_count:
            data.append("\nERRORS")
            data.append("-----------------------------------------------------------------------")
            if self.tba.error_data["ERROR"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.tba.error_data["ERROR"].items() for error in errors]))
            if self.tba.error_data["CRITICAL"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.tba.error_data["CRITICAL"].items() for error in errors]))
            if self.tba.error_data["WARN"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.tba.error_data["WARN"].items() for error in errors]))
            if self.tba.error_data["EMERGENCY"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.tba.error_data["EMERGENCY"].items() for error in errors]))
            if self.tba.error_data["FATAL"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.tba.error_data["FATAL"].items() for error in errors]))
            data.append('\n')
        else:
            data.append("No errors")

        if self.tba.missing_file_data:
            data.append("\nMissing files")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([file for file in self.tba.missing_file_data]))
            data.append('\n')
        else:
            data.append("No missing files")


        diff_trades = self.tba.trade_data['general'] - self.tc_data["tba_trades"]
        diff_loans = self.tba.trade_data['loan'] - self.tc_data["tba_loantrades"]
        diff_repos = self.tba.trade_data['repo'] - self.tc_data["tba_repotrades"]
        if self.tba.trade_data['total'] != self.total_db_trade_counts['tba'] or diff_trades or diff_loans or diff_repos:
            data.append("\nDifferences in Amount between Input and Database")
            data.append("-----------------------------------------------------------------------")
            if diff_trades:
                data.append(f'General: {abs(diff_trades)}')
            if diff_loans:
                data.append(f'Loans: {abs(diff_loans)}')
            if diff_repos:
                data.append(f'Repos: {abs(diff_repos)}')
            data.append('\n')

        if self.duplicate_data['tba_trades']:
            data.append("\nDuplicate data found in TradeBooking.Trades")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['tba_trades']]))
            data.append('\n')

        if self.duplicate_data['tba_loantrades']:
            data.append("\nDuplicate data found in TradeBooking.LoanTrades")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['tba_loantrades']]))
            data.append('\n')

        if self.duplicate_data['tba_repotrades']:
            data.append("\nDuplicate data found in TradeBooking.RepoTrades")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['tba_repotrades']]))
            data.append('\n')

        # if tba_files_no_newline:
        #     data.append("\nFiles with no newline appended at end")
        #     data.append("-----------------------------------------------------------------------")
        #     data.append('\n'.join([file for file in tba_files_no_newline]))
        #     data.append('\n')
        # else:
        #     data.append("All files are appended with newline at end")

        data.append(f"Zip Logs/Input/Output files: {'done' if all(self.tba.archive_data.values()) else 'fail'}")
        data.append('\n')

        data.append("=======================================================================")
        data.append(f"Position Management System (PMA): {pma_color_code}")
        data.append("=======================================================================")
        data.append(f"Log files ({self.pma.count_data['logs']} files total):")
        data.append(f"Generaltrades: {self.pma.trade_data['general']} input trades")
        data.append(f"Repotrades: {self.pma.trade_data['repo']} input trades")
        data.append(f"Loantrades: {self.pma.trade_data['loan']} input trades")
        data.append(f"Loads: {self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'load*.log')} files")
        data.append(f"Eod_extracts: {self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'eod_extract*.log')} files")
        data.append(f"Extract_client_position: {self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'extract_client_position*.log')} file")
        data.append(f"Position_computation: {self.pma.file_checker.get_num_files(self.pma.dirs['logs'], 'position_computation*.log')} file")
        data.append(f"Trades Loaded in DB: {self.total_db_trade_counts['pma']}")
        data.append(f"Trades received in Input: {self.pma.trade_data['total']}")
        data.append(f"Errors in log files: {pma_error_count}")

        if pma_error_count:
            data.append("\nERRORS")
            data.append("-----------------------------------------------------------------------")
            if self.pma.error_data["ERROR"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.pma.error_data["ERROR"].items() for error in errors]))
            if self.pma.error_data["CRITICAL"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.pma.error_data["CRITICAL"].items() for error in errors]))
            if self.pma.error_data["WARN"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.pma.error_data["WARN"].items() for error in errors]))
            if self.pma.error_data["EMERGENCY"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.pma.error_data["EMERGENCY"].items() for error in errors]))
            if self.pma.error_data["FATAL"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.pma.error_data["FATAL"].items() for error in errors]))
            data.append('\n')
        else:
            data.append("No errors")

        if self.pma.missing_file_data:
            data.append("\nMissing files")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([file for file in self.pma.missing_file_data]))
            data.append('\n')
        else:
            data.append("No missing files")

        if self.pma.file_anomalies:
            data.append("\nFile-Anomalies by +/- 20%")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([f"[{size1} bytes : {size2} bytes] {file1} : {file2}" for file1, file2, size1, size2 in self.pma.file_anomalies]))
            data.append('\n')
        else:
            data.append("No file anomalies")

        if self.pma.trade_data['total'] != self.total_db_trade_counts['pma']:
            data.append("\nDifferences in Amount between Input and Database")
            data.append("-----------------------------------------------------------------------")
            diff_trades = self.pma.trade_data['general'] - self.tc_data["pma_trades"]
            diff_loans = self.pma.trade_data['loan'] - self.tc_data["pma_loantrades"]
            diff_repos = self.pma.trade_data['repo'] - self.tc_data["pma_repotrades"]
            if diff_trades:
                data.append(f'General: {abs(diff_trades)}')
            if diff_loans:
                data.append(f'Loans: {abs(diff_loans)}')
            if diff_repos:
                data.append(f'Repos: {abs(diff_repos)}')
            data.append('\n')

        diff_trades = self.tc_data['tba_trades'] - self.tc_data["pma_trades"]
        diff_loans = self.tc_data['tba_loantrades'] - self.tc_data["pma_loantrades"]
        diff_repos = self.tc_data['tba_repotrades'] - self.tc_data["pma_repotrades"]
        if self.total_db_trade_counts['tba'] != self.total_db_trade_counts['pma'] or diff_trades or diff_loans or diff_repos:
            data.append("\nDifferences in Amount between TBA and PMA within Database")
            data.append("-----------------------------------------------------------------------")
            if diff_trades:
                data.append(f'General: {abs(diff_trades)}')
            if diff_loans:
                data.append(f'Loans: {abs(diff_loans)}')
            if diff_repos:
                data.append(f'Repos: {abs(diff_repos)}')
            data.append('\n')

        if self.duplicate_data['pma_trades']:
            data.append("\nDuplicate data found in PoseManagement.Trades")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['pma_trades']]))
            data.append('\n')

        if self.duplicate_data['pma_loantrades']:
            data.append("\nDuplicate data found in PoseManagement.LoanTrades")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['pma_loantrades']]))
            data.append('\n')

        if self.duplicate_data['pma_repotrades']:
            data.append("\nDuplicate data found in PoseManagement.RepoTrades")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['pma_repotrades']]))
            data.append('\n')

        # if pma_files_no_newline:
        #     data.append("\nFiles with no newline appended at end")
        #     data.append("-----------------------------------------------------------------------")
        #     data.append('\n'.join([file for file in pma_files_no_newline]))
        #     data.append('\n')
        # else:
        #     data.append("All files are appended with newline at end")

        data.append(f"Zip Logs/Input/Output files: {'done' if all(self.pma.archive_data.values()) else 'fail'}")
        data.append('\n')

        data.append("=======================================================================")
        data.append(f"Credit Risk System (CRS): {crs_color_code}")
        data.append("=======================================================================")
        data.append(f"Log files ({self.crs.count_data['logs']} files total):")
        data.append(f"Repotrades: {self.crs.trade_data['repo']} input trades")
        data.append(f"Loantrades: {self.crs.trade_data['loan']} input trades")
        data.append(f"Loads: {self.crs.file_checker.get_num_files(self.crs.dirs['logs'], 'load*.log')} files")
        data.append(f"Risks: {self.crs.file_checker.get_num_files(self.crs.dirs['logs'], 'risk*.log')} files")
        data.append(f"Trades Loaded in DB: {self.total_db_trade_counts['crs']}")
        data.append(f"Trades received in Input: {self.crs.trade_data['total']}")
        data.append(f"Errors in log files: {crs_error_count}")

        if crs_error_count:
            data.append("\nERRORS")
            data.append("-----------------------------------------------------------------------")
            if self.crs.error_data["ERROR"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.crs.error_data["ERROR"].items() for error in errors]))
            if self.crs.error_data["CRITICAL"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.crs.error_data["CRITICAL"].items() for error in errors]))
            if self.crs.error_data["WARN"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.crs.error_data["WARN"].items() for error in errors]))
            if self.crs.error_data["EMERGENCY"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.crs.error_data["EMERGENCY"].items() for error in errors]))
            if self.crs.error_data["FATAL"]:
                data.append("\n".join([f"{file}: {error}" for file, errors in self.crs.error_data["FATAL"].items() for error in errors]))
            data.append('\n')
        else:
            data.append("No errors")

        if self.crs.missing_file_data:
            data.append("\nMissing files")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([file for file in self.crs.missing_file_data]))
            data.append('\n')
        else:
            data.append("No missing files")

        if self.crs.file_anomalies:
            data.append("\nFile Anomalies by +/- 20%")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([f"[{size1} bytes : {size2} bytes] {file1} : {file2}" for file1, file2, size1, size2 in self.crs.file_anomalies]))
            data.append('\n')
        else:
            data.append("No file anomalies")

        diff_loans = self.crs.trade_data['loan'] - self.tc_data["crs_loantrades"]
        diff_repos = self.crs.trade_data['repo'] - self.tc_data["crs_repotrades"]
        if self.crs.trade_data['total'] != self.total_db_trade_counts['crs'] or diff_loans or diff_repos:
            data.append("\nDifferences in Amount between Input and Database")
            data.append("-----------------------------------------------------------------------")
            # diff_trades = self.crs.trade_data['general'] - self.tc_data["crs_trades"]
            # if diff_trades:
            #     data.append(f'General: {abs(diff_trades)}')
            if diff_loans:
                data.append(f'Loans: {abs(diff_loans)}')
            if diff_repos:
                data.append(f'Repos: {abs(diff_repos)}')
            data.append('\n')

        if self.tc_data['pma_loantrades'] + self.tc_data['pma_repotrades'] != self.total_db_trade_counts['crs']:
            data.append("\nDifferences in Amount between PMA and CRS within Database")
            data.append("-----------------------------------------------------------------------")
            # diff_trades = self.tc_data['pma_trades'] - self.tc_data["crs_trades"]
            diff_loans = self.tc_data['pma_loantrades'] - self.tc_data["crs_loantrades"]
            diff_repos = self.tc_data['pma_repotrades'] - self.tc_data["crs_repotrades"]
            # if diff_trades:
            #     data.append(f'General: {abs(diff_trades)}')
            if diff_loans:
                data.append(f'Loans: {abs(diff_loans)}')
            if diff_repos:
                data.append(f'Repos: {abs(diff_repos)}')
            data.append('\n')

        if self.duplicate_data['crs_loantrades']:
            data.append("\nDuplicate data found in creditriskdb.BackOffice_Loan")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['crs_loantrades']]))
            data.append('\n')

        if self.duplicate_data['crs_repotrades']:
            data.append("\nDuplicate data found in creditriskdb.BackOffice_Repo")
            data.append("-----------------------------------------------------------------------")
            data.append("\n".join([trade_id for trade_id in self.duplicate_data['crs_repotrades']]))
            data.append('\n')

        # if crs_files_no_newline:
        #     data.append("\nFiles with no newline appended at end")
        #     data.append("-----------------------------------------------------------------------")
        #     data.append('\n'.join([file for file in crs_files_no_newline]))
        #     data.append('\n')
        # else:
        #     data.append("All files are appended with newline at end")

        data.append(f"Zip Logs/Input/Output files: {'done' if all(self.crs.archive_data.values()) else 'fail'}")

        return "\n".join(data)
