from datetime import datetime, date, timedelta
import yfinance as yf
import pandas as pd
import requests
import json
from bcb import sgs as sgs_bcb
from .info import *
from .singleton import Singleton
from .database_components import DatabaseComponents

class Database(DatabaseComponents, metaclass = Singleton): 
    """
    A database class that manages financial data retrieval and processing.
    """

    def __init__(self) -> None:
        """Initializes the Database class with an empty DataFrame and an empty dictionary for tracking dates."""
        self._DATA = pd.DataFrame()
        self._seeken_dates = {}

    def _add_seeken_dates(self, ticker, open_date, close_date):
        """
        Adds the open and close dates for a ticker to the _seeken_dates dictionary.

        Args:
            ticker (str): The ticker symbol.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.
        """   
        dct_dates = {
            'start': open_date,
            'close': close_date
        }
        self._seeken_dates[ticker] = dct_dates

    def _fetch_CDI(self, open_date, close_date):
        """
        Fetches the CDI data from the Brazilian Central Bank API and updates the _DATA DataFrame.

        Args:
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.
        """
        open_str = open_date.strftime('%d/%m/%Y')
        close_str = close_date.strftime('%d/%m/%Y')
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json&dataInicial={open_str}&dataFinal={close_str}'
        response = requests.get(url)
        cdi = response.json()
        dates = []
        values = []
        for cdi_info in cdi:
            day, month, year = cdi_info['data'].split('/')
            date = datetime(int(year), int(month), int(day))
            dates.append(date)
            value = cdi_info['valor']
            values.append(float(value))
        if date < close_date:
            dates.append(close_date)
            values.append(float(value))
        info = {
            'CDI_close': values,
            'date': dates
        }
        df = pd.DataFrame(info)
        df = df.set_index('date')
        raw_date_range = pd.date_range(
                start=open_date, end=close_date+timedelta(days=1))
        date_range = [date for date in raw_date_range if date.weekday() < 5]
        values = []
        dates_to_add = []
        for date in date_range:
            try:
                values.append(df[['CDI_close']].loc[:date].values[-1][-1])
                dates_to_add.append(date)
            except:pass
        df = pd.DataFrame({'CDI_close': values}, index=dates_to_add)
        df = df.loc[open_date:close_date]
        self._DATA = pd.concat([df, self._DATA], axis=1)

    def _fetch_PIB_BR(self, open_date, close_date):
        """
        Fetches the Brazilian GDP (PIB) data from the Brazilian Central Bank API and updates the _DATA DataFrame.

        Args:
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.
        """
        open_str = open_date.strftime('%d/%m/%Y')
        close_str = close_date.strftime('%d/%m/%Y')
        url = f'https://api.bcb.gov.br/dados/serie/bcdata.sgs.24363/dados?formato=json&dataInicial={open_str}&dataFinal={close_str}'
        response = requests.get(url)
        pib = response.json()
        dates = []
        values = []
        for pib_info in pib:
            day, month, year = pib_info['data'].split('/')
            date = datetime(int(year), int(month), int(day))
            dates.append(date)
            value = pib_info['valor']
            values.append(float(value))
        if date < close_date:
            dates.append(close_date)
            values.append(float(value))
        info = {
            'PIBBR_close': values,
            'date': dates
        }
        df = pd.DataFrame(info)
        df = df.set_index('date')
        raw_date_range = pd.date_range(
                start=open_date, end=close_date+timedelta(days=1))
        date_range = [date for date in raw_date_range if date.weekday() < 5]
        values = []
        dates_to_add = []
        for date in date_range:
            try:
                values.append(df[['PIBBR_close']].loc[:date].values[-1][-1])
                dates_to_add.append(date)
            except:pass
        df = pd.DataFrame({'PIBBR_close': values}, index=dates_to_add)
        df = df.loc[open_date:close_date]
        self._DATA = pd.concat([df, self._DATA], axis=1)

        
    def _fetch_sgs(self, ticker, open_date, close_date):
        """
        Fetches data from the SGS (Sistema Gerenciador de Séries Temporais) system of the Brazilian Central Bank
        and updates the _DATA DataFrame.

        Args:
            ticker (str): The ticker symbol corresponding to an SGS code.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.
        """
        code = SGS_INFO[ticker]
        df = sgs_bcb.get(code, start=open_date, end=close_date)
        raw_date_range = pd.date_range(
            start=open_date, end=close_date+timedelta(days=1))
        date_range = [date for date in raw_date_range if date.weekday() < 5]
        values = []
        dates_to_add = []
        for date in date_range:
            try:
                values.append(df[[str(code)]].loc[:date].values[-1][-1])
                dates_to_add.append(date)
            except:
                pass
        df = pd.DataFrame({ticker+'_close': values}, index=dates_to_add)
        df = df.loc[open_date:close_date]
        self._DATA = pd.concat([df, self._DATA], axis=1)

    def _fetch_yf(self, ticker: str, open_date, close_date, interval='1d'):
        """
        Fetches historical price data for a ticker from Yahoo Finance and updates the _DATA DataFrame.

        Args:
            ticker (str): The ticker symbol.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.
            interval (str): The data interval. Defaults to '1d'.

        Returns:
            bool: True if data was successfully fetched, False otherwise.
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
                              start=open_date, end=close_to_seek, progress=False, show_errors=False)
        if len(candles) == 0:
            ticker_yf = ticker
            candles = yf.download(tickers=ticker_yf,
                                  start=open_date, end=close_to_seek, progress=False, show_errors=False)
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

    def _fetch_brapi(self, ticker: str, open_date, close_date):
        """
        Fetches historical price data for a ticker from the BRAPI API and updates the _DATA DataFrame.

        Args:
            ticker (str): The ticker symbol.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.

        Returns:
            bool: True if data was successfully fetched, False otherwise.
        """
        url_request = (
            f"https://brapi.dev/api/quote/{ticker}?=max&interval=1d&fundamental=false")
        rqst = requests.get(url_request)
        obj = json.loads(rqst.text)
        error = obj.get('error')
        if error:
            return False
        data = obj['results'][0]['historicalDataPrice']
        dates = []
        open = []
        close = []
        high = []
        low = []
        volume = []
        for daily_data in data:
            dates.append(daily_data['date'])
            open.append(daily_data['open'])
            close.append(daily_data['Adj Close'])
            high.append(daily_data['high'])
            low.append(daily_data['low'])
            volume.append(daily_data['volume'])
        dates = [datetime.fromtimestamp(ts) for ts in dates]
        dates = [dt.replace(hour=0, minute=0, second=0) for dt in dates]
        dates = pd.to_datetime(dates)
        info_dct = {
            'date': dates,
            ticker+'_open': open,
            ticker+'_high': high,
            ticker+'_low': low,
            ticker+'_close': close,
            ticker+'_volume': volume,
        }
        df = pd.DataFrame(info_dct)
        df = df.set_index('date')
        df = df.loc[open_date:close_date]
        self._DATA = pd.concat([df, self._DATA], axis=1)
        return True

    def _fetch_prices(self, ticker, open_date, close_date):
        """
        Fetches price data for a ticker using Yahoo Finance first, then BRAPI if Yahoo fails, and updates the _DATA DataFrame.

        Args:
            ticker (str): The ticker symbol.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.

        Raises:
            ValueError: If data could not be fetched from both Yahoo Finance and BRAPI.
        """
        data_to_fetch = {
            'ticker': ticker,
            'open_date': open_date,
            'close_date': close_date
        }
        yahoo = self._fetch_yf(**data_to_fetch)
        if not yahoo:
            brapi = self._fetch_brapi(**data_to_fetch)
            if not brapi:
                self._seeken_dates.pop(ticker)
                raise Exception("""No data found for {}!""".format(ticker))

    def _fetch_currencies(self, ticker, open_date, close_date):
        """
        Fetches currency exchange rate data using Yahoo Finance and updates the _DATA DataFrame.

        Args:
            ticker (str): The ticker symbol for the currency.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.
        """
        splited_ticker = ticker.split('/')
        ticker_to_fetch = splited_ticker[0]+splited_ticker[1]+'=X'
        days_to_seek = (pd.to_datetime(date.today()) - open_date).days + 10
        data = yf.download(
            ticker_to_fetch, period=f"{str(days_to_seek)}d", progress=False)
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
        Checks the type of ticker and returns the configuration for fetching data.

        Args:
            ticker (str): The ticker symbol.

        Returns:
            tuple: Configuration details, such as whether the data is for an index, currency, etc.
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
        is_cdi = ticker_splitted[-1] == 'CDI'
        is_pib = ticker_splitted[-1] == 'PIBBR'
        is_sgs = ticker_splitted[-1] in SGS_INFO
        if is_cdi or is_currency or is_pib or is_sgs:
            info_dct['get_prices'] = False
        return info_dct

    def _allow_changes(self, ticker, open_date, close_date):
        """
        Checks if the data for the specified ticker and date range needs to be updated.

        Args:
            ticker (str): The ticker symbol.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.

        Returns:
            tuple: Contains a boolean indicating if data should be fetched, and the adjusted open and close dates.
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
            there_is_transf = ticker_data['transf'] is not None
            if previous_than_start or after_close or there_is_transf:
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
                self._add_seeken_dates(ticker, open_date, close_date)
                return {'changes': True, 'open_date': open_date, 'close_date': close_date}
            else:
                return {'changes': False}
        self._add_seeken_dates(ticker, open_date, close_date)
        return {'changes': True, 'open_date': open_date, 'close_date': close_date}

    def _add_assets(self, ticker: str, open_date, close_date):
        """
        Adds the data for the specified ticker and date range to the internal DataFrame (_DATA).

        Args:
            ticker (str): The ticker symbol.
            open_date (datetime): The start date of the data.
            close_date (datetime): The end date of the data.

        Raises:
            ValueError: If the ticker is not recognized.
        """
        changes_data = self._allow_changes(ticker, open_date, close_date)
        if not changes_data['changes']:
            return
        ticker_data = self._check_index(ticker)
        open_date = changes_data['open_date']
        close_date = changes_data['close_date']
        ticker = ticker_data['ticker']
        if ticker_data['get_prices']:
            self._fetch_prices(ticker, open_date, close_date)
        if ticker_data['currencies']:
            self._fetch_currencies(ticker, open_date, close_date)
        elif ticker_data['ticker'] == 'CDI':
            self._fetch_CDI(open_date, close_date)
        elif ticker_data['ticker'] == 'PIBBR':
            self._fetch_PIB_BR(open_date, close_date)
        elif ticker_data['ticker'] in SGS_INFO:
            self._fetch_sgs(ticker_data['ticker'], open_date, close_date)
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
            data = data.loc[open_date:close_date]
            df = self._fetch_cumulated_returns(data, open_date, close_date)
        elif ticker_data['transf'] == 'CLRET':
            data = data.loc[open_date:close_date]
            df = self._fetch_cumulated_log_returns(data, open_date, close_date)
        if df is not None:
            self._DATA = pd.concat([df, self._DATA], axis=1)

    def get_info(self, 
            tickers, 
            open_date: str = None, 
            close_date: str = None, 
            info='close' or 'ohlcv'):
        """
        Retrieves the specified information (e.g., 'close', 'ohlcv') for a list of tickers.

        Args:
            tickers (list): List of ticker symbols.
            open_date (str, optional): Start date for the data. Defaults to None.
            close_date (str, optional): End date for the data. Defaults to None.
            info (str): The type of information to retrieve. Defaults to 'close'.

        Returns:
            pd.DataFrame: DataFrame containing the requested data.
        """
        if close_date is None:
            close_date = pd.to_datetime(date.today())
        if open_date is None:
            open_date = '1950'
        open_date = pd.to_datetime(open_date)
        close_date = pd.to_datetime(close_date)
        if type(tickers) is str:
            tickers = [tickers]
        tickers_to_display = []
        for ticker in tickers:
            ticker = ticker.upper()
            self._add_assets(ticker, open_date, close_date)
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
        Resets the internal data storage (_DATA) and clears the _seeken_dates dictionary.
        """
        self._DATA = pd.DataFrame()
        self._seeken_dates = {}

    @property
    def data(self):
        """
        Returns the current state of the internal DataFrame storing all the fetched data.

        Returns:
            pd.DataFrame: The internal _DATA DataFrame.
        """
        return self._DATA

