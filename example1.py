from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List
import string

class Trader:
    
    def run(self, state: TradingState):
        # print("traderData: " + state.traderData)
        # print("Observations: " + str(state.observations))

				# Orders to be placed on exchange matching engine
        result = {}
        for product in state.order_depths:
            order_depth: OrderDepth = state.order_depths[product]
            orders: List[Order] = []
            acceptable_price = 10  # Participant should calculate this value
            # print("Acceptable price : " + str(acceptable_price))
            # print("Buy Order depth : " + str(len(order_depth.buy_orders)) + ", Sell order depth : " + str(len(order_depth.sell_orders)))
        
            bid, ask, mid = None, None, None
            bid_amt, ask_amt = None, None

            if len(order_depth.sell_orders) != 0:
                best_ask, best_ask_amount = list(order_depth.sell_orders.items())[0]
                ask, ask_amt = best_ask, best_ask_amount
                if int(best_ask) < acceptable_price:
                    # print("BUY", str(-best_ask_amount) + "x", best_ask)
                    orders.append(Order(product, best_ask, -best_ask_amount))
    
            if len(order_depth.buy_orders) != 0:
                best_bid, best_bid_amount = list(order_depth.buy_orders.items())[0]
                bid, bid_amt = best_bid, best_bid_amount
                if int(best_bid) > acceptable_price:
                    # print("SELL", str(best_bid_amount) + "x", best_bid)
                    orders.append(Order(product, best_bid, -best_bid_amount))
            
            result[product] = orders

            alpha = 69
            mid = (bid + ask)/2

            #print statements:
            print(f"timestamp : {state.timestamp}, ", end='')
            print(f"symbol : {product}, ", end='')
            print(f"alpha : {alpha}, ", end='')
            print(f"mid : {mid}, ", end='')
            print(f"bid : {bid}, ", end='')
            print(f"bid_amt : {bid_amt}, ", end='')
            print(f"ask : {ask}, ", end='')
            print(f"ask_amt : {ask_amt}, ", end='')
            print()
    
		    # String value holding Trader state data required. 
				# It will be delivered as TradingState.traderData on next execution.
        traderData = "SAMPLE" 
        
				# Sample conversion request. Check more details below. 
        conversions = 1
        return result, conversions, traderData