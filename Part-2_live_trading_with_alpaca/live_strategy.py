import csv
import time
from decouple import config
import pandas as pd
from datetime import timedelta

from talipp.indicators import EMA, ADX, MACD
from talipp.ohlcv import OHLCVFactory

from alpaca.data.requests import CryptoBarsRequest, CryptoLatestBarRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide
from alpaca.data.live import CryptoDataStream
from trader import Trader

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='algo_trading.log', level=logging.INFO)

# Get the tokens for API access
api_key = config("ALPACA_KEY")
secret_key = config("ALPACA_SECRET")

symbol = "BTC/USD"

# Instatiate the Clients
crypto_stream = CryptoDataStream(api_key, secret_key)
crypto_client = CryptoHistoricalDataClient()


# Trend-following Strategy
# Keep a running statistics of price and indicators values
# trading signals defined in check_signal function
class Strategy:
    def __init__(self, historical_data, ema, adx, macd):
        self.close_price = historical_data['close']
        self.ema = ema
        self.adx = adx
        self.macd = macd
        # Instantiate Trader object for interacting with Alpaca Trading API
        self.trader = Trader(symbol.replace("/", "")) #For some reason Alpaca order/postions do not have slash in symbols

    # Print the last 5 time intervals prices and indicator values
    def print_recent_history(self):
        df = pd.DataFrame(self.close_price).assign(MACD=self.macd, ADX=self.adx, EMA=self.ema).dropna()
        df.to_csv("running_indicators.csv")
        print(df.tail())
        return df
        
    def check_signal(self):
        price = self.close_price.iloc[-1]
        adx_threshold = 25
        if (
            self.macd[-1].macd > 0 and
            self.adx[-1].adx > adx_threshold and
            self.macd[-1].macd > self.macd[-1].signal and
            self.macd[-2].macd < self.macd[-2].signal # MACD Crossover
            ):

            ## Close any shorting position before buying
            if not self.trader.is_long():
                self.trader.close_position()
            self.trader.execute_order(OrderSide.BUY)

        elif (
              self.macd[-1].macd < 0 and
              self.adx[-1].adx > adx_threshold and
              self.macd[-1].macd < self.macd[-1].signal and
              self.macd[-2].macd > self.macd[-2].signal # MACD Crossover
              ):
            
            ## Close any long position before shorting
            if self.trader.is_long():
                self.trader.close_position()
            self.trader.execute_order(OrderSide.SELL)

        # Closing position logic
        if self.trader.is_long():
            if (price < 0.99 * self.ema[-1] and
                self.macd[-1].macd < 0):
                self.trader.close_position()
        if not self.trader.is_long():
            if (price > 1.01 * self.ema[-1] and
                self.macd[-1].macd > 0):
                self.trader.close_position()  

    async def bar_handler(self, bar):
        print("********************************")
        print(f"Received message: \n{bar}")

        # Expected data: Alpaca.common.models Bar {{'close': 57491.3405, 'high': 57491.3405...} }
        # Input Validation: Check for symbol and close price
        if bar.symbol != symbol:
            print("unexpcted input received from socket: ", bar)
            logger.warning("unexpcted input received from socket: ", bar)
            ## If data from socket is unexpeceted, don't quit, just ignore this message and wait for next one
            return

        filename = "live_bar_data.csv"
        bar_dict = bar.dict()
        # Save to csv
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)

            if csvfile.tell() == 0:
              header = bar_dict.keys()
              writer.writerow(header)
              
            row = bar_dict.values()
            writer.writerow(row)

        #update values
        df_dict = {key:[value] for key, value in bar_dict.items()}
        df = pd.DataFrame((df_dict)).set_index('timestamp')
        self.close_price = pd.concat([self.close_price, df['close']])
        self.ema.add(bar_dict['close'])
        self.macd.add(bar_dict['close'])
        self.adx.add(OHLCVFactory.from_dict(df_dict))
        self.print_recent_history()

        ## checking signals and executing orders if needed
        self.check_signal()
        ## Checing order status from the server after 1 second
        ## Make sure it is successfully filled with the correct quantity
        ## Retry if needed
        time.sleep(1)
        self.trader.validate_past_orders()
        


### In order to calcuate indicators (MACD, EMA, ADX) values for 1-minute interval
### We need to retrieve historical data from past 60 minutes
def get_past_60m_historical_data():
    # Try Catch errors from alpaca servers when retrieving historical data
    try:
        btc_latest = crypto_client.get_crypto_latest_bar(CryptoLatestBarRequest(symbol_or_symbols=symbol))
        # Expected data: dictionary {'BTC/USD': {'close': 57491.3405, 'high': 57491.3405...} }
        # Input Validation: Check for symbol and the existence of most-recent timestamp
        if (symbol not in btc_latest or not btc_latest[symbol].timestamp):
            print("unexpcted input received: ", btc_latest)
            exit(1)

        print("Latest price info:\n", btc_latest)
        print("************************")
        # Use the latest timestamp to get past 60 minutes bar
        latest_timestamp = btc_latest[symbol].timestamp
        start_timestamp = latest_timestamp - timedelta(minutes=60)
        # Creating request object
        request_params = CryptoBarsRequest(
          symbol_or_symbols=symbol,
          timeframe=TimeFrame.Minute,
          start=start_timestamp,
          end=latest_timestamp
        )
        
        # Retrieve 1-minute bars for Bitcoin in a DataFrame and printing it
        btc_bars = crypto_client.get_crypto_bars(request_params)
        # Expected data: Alpaca.data.models.BarSet data={'BTC/USD': [{'close': 57187.2235, 'high': 57187.2235,..}, {'close': 57261.56, 'high': 57261.56, ...}]}
        # Input Validation: Check for symbol and length of past hour of 1-minutes more than 20
        if (symbol not in btc_bars.data or len(btc_bars.data[symbol]) < 20):
            print("unexpcted input received: ", btc_bars)
            exit(1)
    except Exception as e:
        print((f"Error occurred when retrieving historical data:", e))
        logger.exception(f"Error occurred when retrieving historical data:", e)
        # Exit the program without historical data to support trading strategy
        exit(1)
        

    btc_df = btc_bars.df
    btc_df.reset_index("symbol" ,drop=True, inplace=True)
    btc_df = btc_df.resample('1min').ffill()
    if(btc_df.columns.isin(['open','high','low','close','volume']).any()):
        btc_df = btc_df[['open','high','low','close','volume']]
    print("Most recent OHLCV:\n")
    print(btc_df.tail())

    # Calculate all indicators for trading
    ema = EMA(period = 20, input_values = btc_df['close'])
    adx = ADX(di_period = 14, adx_period=14, input_values=OHLCVFactory.from_dict(btc_df))
    macd= MACD(12, 26, 9, input_values = btc_df['close'])
    return btc_df, ema, adx, macd

    
if __name__ == "__main__":  
  historical_data = get_past_60m_historical_data()
  trading_strategy = Strategy(*historical_data)
  
  crypto_stream.subscribe_bars(trading_strategy.bar_handler, symbol)
  crypto_stream.run()