import requests
import pandas as pd
from datetime import date

class AjustesDownloader:
    def __init__(self, verbose=True):
        # Initialize with optional base_url and verbosity flag
        self.verbose = verbose
        self.url_template = "https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-ajustes-do-pregao-ptBR.asp?dData1={data_1}"

    def _is_weekend(self, mDate: date) -> bool:
        # Check if the given date is a weekend (Saturday or Sunday)
        return mDate.weekday() >= 5

    def _url(self, date: date) -> str:
        # Build the URL for the given date and fetch the HTML content
        return self.url_template.format(data_1 = date.strftime("%d/%m/%Y"))


    def _fetch_html(self, mDate:date) -> str:
        try:
            response = requests.get(self._url(mDate))
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            # Print error if verbose and return None on failure
            if self.verbose:
                print(f"Erro ao acessar {self._url(mDate)}: {e}")
            return None

    def _fetch_table(self, mDate : date) -> pd.DataFrame:

        # Parse the HTML table and extract the data into a DataFrame
        try :
            ajustesTables = pd.read_html(self._fetch_html(mDate), thousands='.', decimal=',', attrs={'id': 'tblDadosAjustes'})
        except ValueError as e:
            # Print error if verbose and return None on failure
            if self.verbose:
                if e.args[0] == "No tables found":
                    print(f"Nenhum dado encontrado para a data: {mDate}")
                else:
                    print(f"Erro ao processar a tabela para a data {mDate}: {e}")

            return pd.DataFrame()
            
        # Validates that only one table is found
        if len(ajustesTables) != 1:
            raise ValueError(f"Esperava apenas uma tabela, mas encontrei {len(ajustesTables)} tabelas.")
        else :
            ajustesTable = ajustesTables[0]

        # Rename columns in ajustesTable[0]
        ajustesTable.columns = ["Mercadoria", "Vencimento", "AjusteAnterior", "AjusteAtual", "Variacao", "AjustePorContrato"]
    
        # Apply last observation carried forward (LOCF) to the 'Mercadoria' column if it exists
        ajustesTable['Mercadoria'] = ajustesTable['Mercadoria'].ffill()

        return ajustesTable

    def download(self, mDate: date) -> pd.DataFrame:

        # Main method to download and parse data for a given date
        if self._is_weekend(mDate):
            # Skip weekends
            if self.verbose:
                print(f"Fim de semana - data: {mDate}")
            return pd.DataFrame()

        if self.verbose:
            print(f"Processando a data: {mDate}")

        mTable = self._fetch_table(mDate)
        return mTable

        
if __name__ == "__main__":
    # Example usage
    downloader = AjusteDownloader()
    mDate = date(2023, 6, 26)  # Example date
    mDate = date(2023, 12, 25)  # Example date
    # df = downloader._fetch_html(mDate)
    df = downloader.download(mDate)
    mDate = date(2023, 12, 26)  # Example date
    df = downloader.download(mDate)
    print(df)