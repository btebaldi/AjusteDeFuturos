import os
import pandas as pd
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import AjustesDownloader as DL
from enum import IntEnum, auto

class VerboseLevel(IntEnum):
    NONE = 0
    DEBUG = 1
    ALL = 2

class AjustesProcessor:
    def __init__(self, start_date : datetime.date = None, end_date: datetime.date=None, verbose_level: VerboseLevel = VerboseLevel.NONE):

        self.start_date: datetime.date = start_date or (datetime.date.today() - datetime.timedelta(days=1000))
        self.end_date: datetime.date = end_date or (datetime.date.today() - datetime.timedelta(days=1))
        self.verbose: VerboseLevel = verbose_level

        self.output_dir = "./Database/AjusteDiario_raw"
        self.output_fileMask: str = "Ajustes_{data}.csv"

        self.dateCollection: list[datetime.date] = self.create_data_collection()

    def get_config(self) -> str:
        config_str = (
            "AjustesProcessor Configuration:\n"
            f"  Start Date: {self.start_date}\n"
            f"  End Date: {self.end_date}\n"
            f"  Verbose Level: {self.verbose}\n"
            f"  Output Directory: {self.output_dir}\n"
            f"  Output File Mask: {self.output_fileMask}\n"
            f"  Dates to Process: {len(self.dateCollection)} dates"
        )
        return config_str

    def __str__(self) -> str:
        return ( self.get_config() )
    
    def set_date_range(self, start_date: datetime.date, end_date: datetime.date) -> None:
        """
        Set the start and end dates for processing and update the date collection.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.dateCollection = self.create_data_collection()

    def log(self, message: str, level: VerboseLevel = VerboseLevel.NONE) -> None:
        if self.verbose >= level:
            print(message)

    def check_raw_file_exists(self, mDate: datetime) -> bool:
        """
        Check if the destination file exists.
        """

        mFile = self.output_fileMask.format(data = mDate.strftime("%Y-%m-%d"))
        execution_dir = os.path.dirname(os.path.abspath(__file__))

        mFile_full = os.path.join(execution_dir, self.output_dir, mFile)
        if os.path.exists(mFile_full):
            return True
        else:
            return False
    
    def _is_weekend(self, mDate: datetime) -> bool:
        """
        Check if the given date is a weekend (Saturday or Sunday).
        """
        return mDate.weekday() >= 5

    def create_data_collection(self) -> list[datetime.date]:
        """
        Creates a list of dates between self.date_ini and self.date_fim
        for which the corresponding daily data file exists.
        """
        existing_dates = []
        for mDate in pd.date_range(start = self.start_date, end = self.end_date, freq='D'):
            # if date is weekday skip
            if self._is_weekend(mDate):
                self.log(f"Fim de semana - data: {mDate}", level=VerboseLevel.ALL) 
                continue
            # if day is alread downloaded skip
            elif self.check_raw_file_exists(mDate):
                self.log(f"Dia ja salvado em dados locais para {mDate}", level=VerboseLevel.ALL)
                continue
            self.log(f"Adicionando a data {mDate} na colecao.", level=VerboseLevel.ALL)

            # if day is not weekend and not already downloaded, add to list
            existing_dates.append(mDate)
        
        return existing_dates

    @staticmethod
    def get_num_cores() -> int:
        """
        Returns the number of CPU cores available on the system.

        Returns:
            int: The number of CPU cores detected. If detection fails, returns 1 as a fallback.
        """
        cores:int = 1
        try:
            # os.cpu_count() returns the number of logical CPUs in the system
            cores = os.cpu_count()
            if cores is None:
                # In rare cases, os.cpu_count() may return None
                cores = 1
        except Exception:
            # If any exception occurs during detection, fallback to 1
            cores = 1
        
        # self.log("") # (f"Detected number of cores: {get_num_cores()}")
    
        return cores

    def download_data(self) -> None:
        
        # Suponha que você já tenha isso:
        downloader = DL.AjustesDownloader(verbose=False)
        os.makedirs(self.output_dir, exist_ok = True)

        with ThreadPoolExecutor(max_workers = self.get_num_cores()) as executor:
            future_to_date = {executor.submit(downloader.download, d): d for d in self.dateCollection}
            for future in as_completed(future_to_date):
                df = future.result()
                fileName = self.output_fileMask.format(data = future_to_date[future].strftime("%Y-%m-%d"))
                if not df.empty:
                    df.to_csv(os.path.join(self.output_dir, fileName), index = False)
                
                self.log(f"{fileName} processado.", level=1)
                    
                    # df.to_feather(os.path.join(output_dir, f"swapdi_{future_to_date[future]}.feather"))
        self.log("Download completo.", level=1)


    def download_data_single(self) -> None:
        
        # Suponha que você já tenha isso:
        downloader = DL.AjustesDownloader(verbose=False)
        os.makedirs(self.output_dir, exist_ok = True)
        
        

        for mDate in self.dateCollection:
            df = downloader.download(mDate)
            fileName = self.output_fileMask.format(data = mDate.strftime("%Y-%m-%d"))
            if not df.empty:
                df.to_csv(os.path.join(self.output_dir, fileName), index = False)
            self.log(f"{fileName} processado.", level = VerboseLevel.DEBUG)
                    
        self.log("Download completo.", level=1)