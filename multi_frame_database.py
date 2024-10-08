from datetime import date, timedelta
import yfinance as yf
import pandas as pd
from .singleton import Singleton
from .database_components import *
from .info import *

class MultiFrameDatabase(DatabaseComponents, metaclass = Singleton):
    def __init__(self)-> None:
        self._DATA = pd.DataFrame()
        self._seeken_dates = {}

    def _add_seeken_dates(self, ticker, open_date, close_date, interval):
        """
        Adds the date range and interval for a ticker to the _seeken_dates dictionary.

        Parameters
        ----------
        ticker : str
            The ticker symbol for the asset.
        open_date : datetime
            The start date of the data range.
        close_date : datetime
            The end date of the data range.
        interval : str
            The data interval (e.g., '1m', '5m').
        """
        dct_dates = {
            'start': open_date,
            'close': close_date,
            "interval": interval
        }
        self._seeken_dates[ticker] = dct_dates

    def _fetch_yf(self, ticker: str, interval, open_date, close_date):
        """
        Fetches data from Yahoo Finance for the specified ticker, date range, and interval.
        
        Parameters
        ----------
        ticker : str
            The ticker symbol for the asset.
        interval : str
            The data interval (e.g., '1m', '5m').
        open_date : datetime
            The start date of the data range.
        close_date : datetime
            The end date of the data range.

        Returns
        -------
        bool
            True if data is successfully fetched, False otherwise.
        """
        ticker_yf = ticker+'.SA'
        if ticker == 'IBOV':
            ticker_yf = "^BVSP"
        elif ticker == 'DJI':
            ticker_yf = "^DJI"
        elif ticker == 'SPX':
            ticker_yf = "^GSPC"
        elif ticker == 'NASDAQ':
            ticker_yf = "^IXIC"
        close_to_seek = close_date + timedelta(days=10)
        candles = yf.download(tickers=ticker_yf,
                              start=open_date, end=close_to_seek, interval=interval, progress=False, show_errors=False)
        if len(candles) == 0:
            ticker_yf = ticker
            candles = yf.download(tickers=ticker_yf,
                                  start=open_date, end=close_to_seek, interval=interval, progress=False, show_errors=False)
            if len(candles) == 0:
                return False
        candles = candles.rename(
            columns={'Open': ticker+'_open', 'High': ticker + '_high',
                     'Low': ticker + '_low', 'Adj Close': ticker + '_close',
                     'Volume': ticker + '_volume'})
        candles.index.names = ['date']
        candles = candles.tz_localize(None)
        candles = candles[[ticker+'_close', ticker+'_open',
                           ticker+'_high', ticker+'_low', ticker+'_volume']]
        self._DATA = pd.concat([candles, self._DATA], axis=1)
        return True

    def _fetch_prices(self, ticker, interval, open_date, close_date):
        """
        Fetches the price data for the given ticker and raises an exception if no data is found.
        
        Parameters
        ----------
        ticker : str
            The ticker symbol for the asset.
        interval : str
            The data interval (e.g., '1m', '5m').
        open_date : datetime
            The start date of the data range.
        close_date : datetime
            The end date of the data range.

        Raises
        ------
        Exception
            If no data is found for the specified ticker.
        """
        data_to_fetch = {
            'ticker': ticker,
            'interval': interval,
            'open_date': open_date,
            'close_date': close_date
        }
        yahoo = self._fetch_yf(**data_to_fetch)
        if not yahoo:
            self._seeken_dates.pop(ticker)
            raise Exception("""No data found for {}!""".format(ticker))

    def _fetch_currencies(self, ticker, interval, open_date, close_date):
        """
        Fetches currency exchange data for the specified ticker.
        
        Parameters
        ----------
        ticker : str
            The currency pair symbol (e.g., 'USD/BRL').
        interval : str
            The data interval (e.g., '1m', '5m').
        open_date : datetime
            The start date of the data range.
        close_date : datetime
            The end date of the data range.
        """
        splited_ticker = ticker.split('/')
        ticker_to_fetch = splited_ticker[0]+splited_ticker[1]+'=X'
        days_to_seek = (pd.to_datetime(date.today()) - open_date).days + 10
        data = yf.download(
            ticker_to_fetch, period=f"{str(days_to_seek)}d", interval=interval, progress=False, show_errors=False)
        data = data.rename(
            columns={'Open': ticker+'_open', 'High': ticker + '_high',
                     'Low': ticker + '_low', 'Close': ticker + '_close',
                     'Volume': ticker + '_volume'})
        data.index.names = ['date']
        data = data.tz_localize(None)
        data = data[[ticker+'_close', ticker+'_open',
                     ticker+'_high', ticker+'_low', ticker+'_volume']]
        self._DATA = pd.concat([data, self._DATA], axis=1)

    def _check_index(self, ticker):
        """
        Analyzes the ticker and returns a dictionary with relevant information about it.
        
        Parameters
        ----------
        ticker : str
            The ticker symbol or currency pair.

        Returns
        -------
        dict
            A dictionary containing information about the ticker, such as whether it 
            is a currency, if transformations are needed, and other relevant details.
        """
        info_dct = {'ticker': ticker, 'transf': None,
                    'get_prices': True, 'previous_days': None, 'currencies': False}
        ticker_splitted = ticker.split('_')
        if ticker_splitted[0][:3] == 'VOL':
            info_dct['ticker'] = ticker_splitted[1]
            info_dct['transf'] = 'VOL'
            info_dct['periods'] = int(ticker_splitted[0][3:])
            info_dct['previous_days'] = (int(ticker_splitted[0][3:])+150)
        elif ticker_splitted[0] == 'RET' or ticker_splitted[0] == 'LRET':
            info_dct['ticker'] = ticker_splitted[1]
            info_dct['transf'] = ticker_splitted[0]
            info_dct['previous_days'] = None
        elif ticker_splitted[0] == 'CRET' or ticker_splitted[0] == 'CLRET':
            info_dct['ticker'] = ticker_splitted[1]
            info_dct['transf'] = ticker_splitted[0]
            info_dct['previous_days'] = None
        is_currency = len(ticker.split('/')) > 1
        if is_currency:
            info_dct['currencies'] = True
        if is_currency:
            info_dct['get_prices'] = False
        return info_dct

    def _allow_changes(self, ticker, interval, open_date, close_date):
        """
        Determines whether changes are needed for the specified ticker based on 
        the existing data and the requested date range and interval.
        
        Parameters
        ----------
        ticker : str
            The ticker symbol for the asset.
        interval : str
            The data interval (e.g., '1m', '5m').
        open_date : datetime
            The start date of the data range.
        close_date : datetime
            The end date of the data range.

        Returns
        -------
        dict
            A dictionary indicating whether changes are needed and the updated date range.
        """
        ticker_data = self._check_index(ticker)
        if ticker_data['previous_days'] is not None:
            open_date = pd.to_datetime(
                open_date) - timedelta(days=(ticker_data['previous_days']))
        ticker = ticker_data['ticker']
        if ticker in self._seeken_dates.keys():
            previous_than_start = self._seeken_dates[ticker]['start'] > pd.to_datetime(
                open_date)
            after_close = self._seeken_dates[ticker]['close'] < pd.to_datetime(
                close_date)
            different_interval = self._seeken_dates[ticker]['interval'] != interval
            there_is_transf = ticker_data['transf'] is not None
            if previous_than_start or after_close or there_is_transf or different_interval:
                self._DATA = self._DATA.drop(columns=[ticker+'_close'])
                try:
                    self._DATA = self._DATA.drop(columns=[ticker+'_open',
                                                          ticker+'_high', ticker+'_low', ticker+'_volume'])
                except:
                    pass
                try:
                    self._DATA = self._DATA.drop(
                        columns=[ticker_data['transf']+'_'+ticker+'_close'])
                except:
                    pass
                try:
                    self._DATA = self._DATA.drop(
                        columns=[ticker_data['transf']+str(ticker_data['periods'])+'_'+ticker+'_close'])
                except:
                    pass
                open_date = min(
                    self._seeken_dates[ticker]['start'], pd.to_datetime(open_date))
                close_date = max(
                    self._seeken_dates[ticker]['close'], pd.to_datetime(close_date))
                self._add_seeken_dates(ticker, open_date, close_date, interval)
                return {'changes': True, 'open_date': open_date, 'close_date': close_date}
            else:
                return {'changes': False}
        self._add_seeken_dates(ticker, open_date, close_date, interval)
        return {'changes': True, 'open_date': open_date, 'close_date': close_date}

    def _add_assets(self, ticker: str, interval: str, open_date, close_date):
        """
        Adds the data for a specific asset to the database, applying necessary transformations.

        Parameters
        ----------
        ticker : str
            The ticker symbol for the asset.
        interval : str
            The data interval (e.g., '1m', '5m').
        open_date : datetime
            The start date of the data range.
        close_date : datetime
            The end date of the data range.
        """
        changes_data = self._allow_changes(ticker, interval, open_date, close_date)
        if not changes_data['changes']:
            return
        ticker_data = self._check_index(ticker)
        open_date = changes_data['open_date']
        close_date = changes_data['close_date']
        ticker = ticker_data['ticker']
        if ticker_data['get_prices']:
            self._fetch_prices(ticker, interval, open_date, close_date)
        if ticker_data['currencies']:
            self._fetch_currencies(ticker, interval, open_date, close_date)
        df = None
        data = self._DATA[[ticker+'_close']].dropna()
        if ticker_data['transf'] == 'VOL':
            df = self._fetch_volatility(
                data, ticker_data['periods'], open_date, close_date)
        elif ticker_data['transf'] == 'RET':
            df = self._fetch_returns(data, open_date, close_date)
        elif ticker_data['transf'] == 'LRET':
            df = self._fetch_log_returns(data, open_date, close_date)
        elif ticker_data['transf'] == 'CRET':
            df = self._fetch_cumulated_returns(data, open_date, close_date)
        elif ticker_data['transf'] == 'CLRET':
            df = self._fetch_cumulated_log_returns(data, open_date, close_date)
        if df is not None:
            self._DATA = pd.concat([df, self._DATA], axis=1)

    def get_info(self, tickers,
                 interval='1m',
                 open_date: str = None,
                 close_date: str = None,
                 info='close' or 'ohlcv'):
        """
        Retrieves the requested information for the specified tickers.
        
        Returns the data within the given date range and interval.

        Parameters
        ----------
        tickers : str or list
            The ticker(s) for which information is requested.
        interval : str, optional
            The data interval (default is '1m').
        open_date : str, optional
            The start date of the data range (default is None).
        close_date : str, optional
            The end date of the data range (default is None).
        info : str, optional
            The type of information requested (default is 'close' or 'ohlcv').

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the requested data.
        """
        if interval not in AVAILABLE_TIME_FRAMES:
            raise Exception("""The interval {} is not available!""".format(interval))
        today = pd.to_datetime(date.today())
        if close_date is None:
            close_date = today
        if open_date is None:
            open_date = today - timedelta(59)
        open_date = pd.to_datetime(open_date)
        close_date = pd.to_datetime(close_date)
        if interval == '1m' and (today - timedelta(days=6)) > open_date:
            open_date = today - timedelta(days=6)
        elif interval == '5m' and (today - timedelta(days=59)) > open_date:
            open_date = today - timedelta(days=59)
        if type(tickers) is str:
            tickers = [tickers]
        tickers_to_display = []
        for ticker in tickers:
            ticker = ticker.upper()
            self._add_assets(ticker, interval, open_date, close_date)
            if info == 'ohlcv':
                tickers_to_display += [ticker+'_open', ticker+'_high',
                                       ticker+'_low', ticker+'_close',
                                       ticker+'_volume']
            else:
                tickers_to_display += [ticker+'_'+info]
        try:
            info_to_return = self._DATA[tickers_to_display].loc[open_date:close_date]
        except:
            info_to_return = self._DATA[tickers_to_display].dropna()
            info_to_return = info_to_return.loc[open_date:close_date]
        if len(info_to_return) == 0:
            raise Exception("""No data found for {}!""".format(tickers))
        return info_to_return

    def reset(self):
        """
        Resets the database by clearing all data and the _seeken_dates dictionary.
        """
        self._DATA = pd.DataFrame()
        self._seeken_dates = {}

    @property
    def data(self):
        """
        Returns the current state of the _DATA DataFrame.
        """
        return self._DATA
