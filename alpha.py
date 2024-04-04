from datamodel import OrderDepth, UserId, TradingState, Order
from typing import List
import string
import jsonpickle

class Trader:
    
    def run(self, state: TradingState):
        # print("traderData: " + state.traderData)
        # print("Observations: " + str(state.observations))

		# Orders to be placed on exchange matching engine
        result = {}


        if TradingState.traderData.startswith("{") and TradingState.traderData.endsswith("}"):
            data = jsonpickle.decode(TradingState.traderData)
        else:
            data = 69
            # data = {}
            # for product in state.order_depths:
            #     # data[product] = {"alpha1" : 0, "alpha2" : 0, "alpha3" : 0, "price_qty_buy" : 0,  "price_qty_sell" : 0}
            #     data[product] = 69
            


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

            # alpha updates:
            # buy_price, sell_price = [], []
            # buy_qty, buy_price = [], []
            # for price,qty in state.order_depths[product].buy_orders.items():
            #     buy_price.append(price)
            #     buy_qty.append(qty)
            # for price, qty in state.order_depths[product].sell_orders.items():
            #     sell_price.append(price)
            #     sell_qty.append(-qty)

            # price_qty_buy = abs(buy_price*buy_qty) 
            # price_qty_sell = abs(sell_price*sell_qty)

            # price_qty_buy_diff = price_qty_buy - data[product]["price_qty_buy"]
            # price_qty_sell_diff = price_qty_sell - data[product]["price_qty_sell"]

            # data[product]["price_qty_buy"] = price_qty_buy
            # data[product]["price_qty_sell"] = price_qty_sell

            # data[product]["alpha1"] = 0.7*data[product]["alpha1"] + 0.3*price_qty_buy_diff
            # data[product]["alpha2"] = 0.7*data[product]["alpha2"] + 0.3*price_qty_sell_diff
            # data[product]["alpha3"] = 0.6*data[product]["alpha3"] + 0.4*(price_qty_buy_diff - price_qty_sell_diff)


            mid = (bid + ask)/2

            #print statements:
            print(f"timestamp : {state.timestamp}, ", end='')
            print(f"symbol : {product}, ", end='')
            # print(f"price_qty_buy : {price_qty_buy}, ", end='')
            # print(f"price_qty_sell : {price_qty_sell}, ", end='')
            # print(f"price_qty_buy_diff : {price_qty_buy_diff}, ", end='')
            # print(f"price_qty_sell_diff : {price_qty_sell_diff}, ", end='')
            # print(f"alpha1 : {alpha1}, ", end='')
            # print(f"alpha2 : {alpha2}, ", end='')
            # print(f"alpha3 : {alpha3}, ", end='')
            print(f"mid : {mid}, ", end='')
            print(f"bid : {bid}, ", end='')
            print(f"bid_amt : {bid_amt}, ", end='')
            print(f"ask : {ask}, ", end='')
            print(f"ask_amt : {ask_amt}, ", end='')
            print()
    
		    # String value holding Trader state data required. 
				# It will be delivered as TradingState.traderData on next execution.
        traderData = jsonpickle.encode(data)
        
				# Sample conversion request. Check more details below. 
        conversions = 1
        return result, conversions, traderData