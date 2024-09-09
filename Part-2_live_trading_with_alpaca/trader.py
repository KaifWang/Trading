from decouple import config
import pandas as pd

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import  MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType

import logging

logger = logging.getLogger(__name__)

pd.set_option("display.max_columns", None)
# Get the tokens for API access
api_key = config("ALPACA_KEY")
secret_key = config("ALPACA_SECRET")

api = TradingClient(api_key, secret_key, raw_data=True) # raw_data = True returns python dict and not convert the output into object

## Our Trading Agent
## This class handles all order execution with alpaca API 
## As well as position tracking if the submitted orders are filled
class Trader:
    
    quantity = 0.01
    # Instatiate the trading client
    def __init__(self, symbol):
        self.symbol = symbol
        #Empty out all positions before starts to trade

        api.close_all_positions(cancel_orders = True)
        # Keep a ruuning record of executed orders for verification purpose later
        self.running_orders = []
        self.failed_orders = []


    def execute_order(self, order_side):

        ## Alpaca does not allow shorting Cryptocurrency. This is difficult to workaround
        ## My solution: whenever shorting, instead of working with BTCUSD, I will be buying another ticker ETHUSD
        ## This is a workaround just to demonstrate my code work for both long/short (no practical business logic for P&L)
        ## By this logic, it means I can only have either BTCUSD or ETHUSD in my porfolio at one time, not both
        if(order_side == OrderSide.SELL):
            self.symbol = "ETHUSD"
        else:
            self.symbol = "BTCUSD"

        # Handle failure during order execution
        try:
            print(f"{order_side}...\n")
            order_result = api.submit_order(MarketOrderRequest(symbol = self.symbol, 
                                                               qty = self.quantity, 
                                                               side = OrderSide.BUY,
                                                               type = OrderType.MARKET,
                                                               time_in_force = TimeInForce.GTC))
            print(order_result)
            print(f"{order_side} {self.quantity} share(s) of {self.symbol}")
            logger.info(f"{order_side} {self.quantity} share(s) of {self.symbol}\n {order_result}")

            ## Record the order, check later
            self.running_orders.append(order_result['id'])
            return order_result
        except Exception as e:
            print((f"Error occurred for {order_side} order:", e))
            logger.exception(f"Error occurred for {order_side} order:", e)
            return False
        
        
    def close_position(self):
        # Handle failure during order execution
        try:
            print("closing position......\n")
            closing_result = api.close_all_positions()
            print(closing_result)
            print(f"Closed position {self.symbol}")
            logger.info(f"Close position {self.symbol}\n {closing_result}")
            return closing_result
        except Exception as e:
            print((f"Error occurred during closing:", e))
            logger.exception(f"Error occurred during closing:", e)
            return False
    

    # Check all past orders on the server to verify if they are filled successfully
    # Retry failed orders
    def validate_past_orders(self):
        try:
            for order_id in self.running_orders:
                order = api.get_order_by_id(order_id)
                print(f"Order id: {order_id} Order status: {order['status']} Order quanty: {order['filled_qty']}")
                if (order['status'] != "filled" or float(order['filled_qty']) != self.quantity):
                    print(f"Failed order detected:", order)
                    logger.warning("Failed order detected:", order)
                    self.failed_orders.append(order)

            self.running_orders = []
            ###Retry failed orders
            if self.failed_orders:
                for order in self.failed_orders:
                    ## This is the workaround logic to retry execution of the intended order 
                    order_side = OrderSide.BUY if order['symbol'].startswith("BTC") else OrderSide.SELL
                    self.execute_order(order_side)
            self.failed_orders = []
        except Exception as e:
            print((f"Error occurred during validating_past_orders:", e))
            logger.exception(f"Error occurred during validating_past_orders:", e)
            return False
            
            
    def is_long(self):
        return self.symbol == "BTCUSD"
    
        