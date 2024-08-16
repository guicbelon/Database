import numpy as np
from datetime import timedelta, date
import yfinance as yf
import requests
import pandas as pd
import json

class DatabaseComponents:
    """
    A class that contains methods to retrieve and process financial data, including
    returns, volatility, and ticker information from various sources.
    """

    def __init__(self) -> None:
        pass

    def _fetch_returns(self, data, open_date, close_date):
        """
        Computes and returns daily returns for a given dataset.
        
        Parameters
        ----------
        data : DataFrame
            The dataset containing price information.
        open_date : str
            The start date for the returns calculation.
        close_date : str
            The end date for the returns calculation.
            
        Returns
        -------
        DataFrame
            A DataFrame containing the daily returns.
        """
        ticker = (data.columns[0]).split('_')[0]
        data = data.dropna()
        daily_returns = (data - data.shift(1)) / data.shift(1)
        daily_returns = daily_returns.fillna(0)
        df = pd.DataFrame(daily_returns)
        df = df.rename(columns={ticker + '_close': 'RET_' + ticker + '_close'})
        df = df.loc[open_date:close_date]
        return df

    def _fetch_log_returns(self, data, open_date, close_date):
        """
        Computes and returns daily logarithmic returns for a given dataset.
        
        Parameters
        ----------
        data : DataFrame
            The dataset containing price information.
        open_date : str
            The start date for the returns calculation.
        close_date : str
            The end date for the returns calculation.
            
        Returns
        -------
        DataFrame
            A DataFrame containing the daily logarithmic returns.
        """
        ticker = (data.columns[0]).split('_')[0]
        data = data.dropna()
        daily_returns = np.log(data / data.shift(1))
        daily_returns = daily_returns.fillna(0)
        df = pd.DataFrame(daily_returns)
        df = df.rename(columns={ticker + '_close': 'LRET_' + ticker + '_close'})
        df = df.loc[open_date:close_date]
        return df

    def _fetch_cumulated_returns(self, data, open_date, close_date):
        """
        Computes and returns accumulated returns for a given dataset.
        
        Parameters
        ----------
        data : DataFrame
            The dataset containing price information.
        open_date : str
            The start date for the returns calculation.
        close_date : str
            The end date for the returns calculation.
            
        Returns
        -------
        DataFrame
            A DataFrame containing the accumulated returns.
        """
        ticker = (data.columns[0]).split('_')[0]
        data = data.dropna()
        daily_returns = (data - data.shift(1)) / data.shift(1)
        cumulated_returns = (daily_returns.fillna(0) + 1).cumprod() - 1
        df = pd.DataFrame(cumulated_returns)
        df = df.rename(columns={ticker + '_close': 'CRET_' + ticker + '_close'})
        df = df.loc[open_date:close_date]
        return df

    def _fetch_cumulated_log_returns(self, data, open_date, close_date):
        """
        Computes and returns accumulated logarithmic returns for a given dataset.
        
        Parameters
        ----------
        data : DataFrame
            The dataset containing price information.
        open_date : str
            The start date for the returns calculation.
        close_date : str
            The end date for the returns calculation.
            
        Returns
        -------
        DataFrame
            A DataFrame containing the accumulated logarithmic returns.
        """
        ticker = (data.columns[0]).split('_')[0]
        data = data.dropna()
        daily_returns = np.log(data / data.shift(1))
        cumulated_returns = daily_returns.fillna(0).cumsum()
        df = pd.DataFrame(cumulated_returns)
        df = df.rename(columns={ticker + '_close': 'CLRET_' + ticker + '_close'})
        df = df.loc[open_date:close_date]
        return df

    def _fetch_volatility(self, data, periods, open_date, close_date):
        """
        Computes and returns the volatility for a given dataset over specified periods.
        
        Parameters
        ----------
        data : DataFrame
            The dataset containing price information.
        periods : int
            The number of periods over which to calculate the rolling volatility.
        open_date : str
            The start date for the volatility calculation.
        close_date : str
            The end date for the volatility calculation.
            
        Returns
        -------
        DataFrame
            A DataFrame containing the rolling volatility.
        """
        ticker = (data.columns[0]).split('_')[0]
        data = data.dropna()
        daily_returns = (data - data.shift(1)) / data.shift(1)
        volatility = daily_returns.rolling(periods).std() * (periods ** 0.5)
        df = pd.DataFrame(volatility)
        df = df.rename(columns={ticker + '_close': 'VOL' + str(periods) + '_' + ticker + '_close'})
        df = df.loc[open_date:close_date]
        return df
    
    def get_brazilian_tickers(self):
        """
        Retrieves a list of Brazilian stock tickers from the BRAPI API.
        
        Returns
        -------
        list or bool
            A list of Brazilian stock tickers if successful, otherwise False.
        """
        url_request = "https://brapi.dev/api/available"
        rqst = requests.get(url_request)
        obj = json.loads(rqst.text)
        error = obj.get('error')
        if error:
            return False
        data = obj['stocks']
        return data
    
    def get_most_traded(self, br_tickers_raw=None, maximum_date=None, 
                        previous_days_to_consider: int = 30, 
                        number_of_tickers: int = 100, volume_filter:float=None,
                        filter_etf_bdr: bool = True):
        """
        Retrieves the most traded Brazilian stock tickers based on trading volume within a specified date range.
        
        Parameters
        ----------
        br_tickers_raw : list, optional
            A list of raw Brazilian stock tickers. If not provided, it will be fetched using get_brazilian_tickers().
        maximum_date : datetime, optional
            The end date for the period to consider. Defaults to today's date.
        previous_days_to_consider : int, optional
            The number of days prior to maximum_date to consider. Defaults to 30 days.
        number_of_tickers : int, optional
            The number of tickers to return. Defaults to 100.
        volume_filter : float, optional
            The minimum volume threshold for a stock to be considered. Defaults to None.
        filter_etf_bdr : bool, optional
            Whether to filter out ETFs and BDRs. Defaults to True.
        
        Returns
        -------
        list
            A list of the most traded Brazilian stock tickers.
        """
        if maximum_date is None:
            maximum_date = pd.to_datetime(date.today())
        open_date = maximum_date - timedelta(days=previous_days_to_consider)
        if br_tickers_raw is None:
            br_tickers_raw = self.get_brazilian_tickers()
        tickers_filtered = []
        if filter_etf_bdr:
            for ticker in br_tickers_raw:
                try:
                    is_ETF_BDR = int(ticker[-2:])
                    is_ETF_BDR = True
                except:
                    is_ETF_BDR = False
                try:
                    is_available = int(ticker[-1])
                    is_available = True
                except:
                    is_available = False
                if not is_ETF_BDR and is_available:
                    tickers_filtered.append(ticker)
        else:
            tickers_filtered = br_tickers_raw          
        br_tickers = [ticker + ".SA" for ticker in tickers_filtered]
        df = yf.download(br_tickers, start=open_date,
                        end=maximum_date, progress=False, show_errors=False)
        volume_info = dict(df['Volume'].sum())
        ordered_volume = dict(
            sorted(volume_info.items(), key=lambda item: item[1], reverse=True))
        if volume_filter is not None:
            ordered_volume = {key: value for key, value in ordered_volume.items() if value >= volume_filter}
        tickers_raw = list(ordered_volume.keys())[:number_of_tickers]
        tickers = [ticker.replace(".SA", "") for ticker in tickers_raw]
        return tickers

    def get_tickers_from_sector(self, sector_name: str = None):
        """
        Retrieves a list of stock tickers grouped by sector, optionally filtering by a specific sector.
        
        Parameters
        ----------
        sector_name : str, optional
            The name of the sector to filter by. If not provided, returns tickers for all sectors.
            
        Returns
        -------
        list or dict
            A list of tickers in the specified sector, or a dictionary of all sectors if no sector is specified.
        """
        url = "https://brapi.dev/api/quote/list"
        params = {
            'sortBy': 'close',
            'sortOrder': 'desc',
        }
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            stocks = data['stocks']
            sector_data = {}
            for stock in stocks:
                sector = stock['sector']
                if sector is not None:
                    ticker = stock['stock']
                    try:
                        sector_data[sector].append(ticker)
                    except:
                        sector_data[sector] = [ticker]
            if sector_name is not None:
                return sector_data[sector_name]
            return sector_data
        else:
            print(f"Request failed with status code {response.status_code}")