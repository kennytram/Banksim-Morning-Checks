import ZipManager
import FileChecker


class ApplicationHealthCheck:

    def __init__(self, base_dir):
        self.base_dir = base_dir
        self.logs_dir = f"{base_dir}/banksimlogs"
        self.file_checker = FileChecker()
        self.data = dict()
        self.zip_manager = ZipManager()

    def archive(self):
        self.zip_manager.archive([], [])


class TBAHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir):
        super().__init__(base_dir)
        self.name = "Trade Booking App"
        self.symbol = "TBA"
        self.base_dir = f"{super().base_dir}/{self.symbol.lower()}"


class PMAHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir):
        super().__init__(base_dir)
        self.name = "Position Management App"
        self.symbol = "PMA"
        self.base_dir = f"{super().base_dir}/{self.symbol.lower()}"


class CRSHealthCheck(ApplicationHealthCheck):

    def __init__(self, base_dir):
        super().__init__(base_dir)
        self.name = "Credit Risk System"
        self.symbol = "CRS"
        self.base_dir = f"{super().base_dir}/{self.symbol.lower()}"
