import pandas as pd
import numpy as np
import ast


data_all = {'AMETHYSTS' : {"alpha_buy_diff" : 0,
                           "alpha_buy_diff_norm" : 0, 
                           "alpha_sell_diff" : 0, 
                           "alpha_sell_diff_norm" : 0, 
                           "momentum" : 0,
                           "price_qty_buy" : 0,
                           "price_qty_sell" : 0,
                           "alpha_trades" : 0,
                           "alpha_tw" : 0,
                           "position" : 0,
                           "pnl" : 0,
                           }, 

            'STARFRUIT' : {"alpha_buy_diff" : 0, 
                           "alpha_buy_diff_norm" : 0, 
                           "alpha_sell_diff" : 0, 
                           "alpha_sell_diff_norm" : 0,
                           "alpha_sell_diff" : 0, 
                           "momentum" : 0,
                           "price_qty_buy" : 0,
                           "price_qty_sell" : 0,
                           "alpha_trades" : 0,
                           "alpha_tw" : 0,
                           "position" : 0,
                           "pnl" : 0,
                           }
            }

def process_order_book_row(row, logging_data):
    data = data_all[row['product']]
    price_qty_buy = abs(row['bid_price_1']*row['bid_volume_1']) 
    price_qty_sell = abs(row['ask_price_1']*row['ask_volume_1'])

    price_qty_buy_diff = price_qty_buy - data["price_qty_buy"]
    price_qty_sell_diff = price_qty_sell - data["price_qty_sell"]

    data["price_qty_buy"] = price_qty_buy
    data["price_qty_sell"] = price_qty_sell

    data['alpha_buy_diff'] = 0.6*data['alpha_buy_diff'] + 0.4*price_qty_buy_diff
    data['alpha_sell_diff'] = 0.6*data['alpha_sell_diff'] + 0.4*price_qty_sell_diff

    trades = [(np.float64(item[0]), np.float64(item[1])) for item in ast.literal_eval(row['trades'])]

    trade_val, tw_val = 0,0
    for price,qty in trades:
        if price == row['bid_price_1']:
            trade_val -= qty
            tw_val -= qty/max(0.5,row['bid_volume_1'])
        elif price == row['ask_price_1']:
            trade_val += qty
            tw_val += qty/max(0.5,row['ask_volume_1'])
    data["alpha_trades"] = 0.5*data["alpha_trades"] + 0.5*trade_val
    data["alpha_tw"] = 0.5*data["alpha_tw"] + 0.5*tw_val




    # order logic.
    order_qty = 0

    if order_qty > 0:
        for i in range(1,4):
            val = min(20 - data["position"],order_qty,row[f"ask_volume_{i}"])
            data["position"] += val
            data["pnl"] -= row[f"ask_price_{i}"]*val
    elif order_qty < 0:
        for i in range(1,4):
            val = min(20 + data["position"],-order_qty,row[f"bid_volume_{i}"])
            data["position"] -= val
            data["pnl"] += row[f"bid_price_{i}"]*val

       

    row['alpha_buy_diff'] = data['alpha_buy_diff']
    row['alpha_sell_diff'] = data['alpha_sell_diff']
    row['alpha_trades'] = data['alpha_trades']
    row['alpha_tw'] = data['alpha_tw']
    row['position'] = data['position']
    row['pnl'] = data['pnl'] + row['mid_price']*data['position']


    
    # Append the updated row to the logging data DataFrame
    return pd.concat([logging_data, row.to_frame().T], ignore_index=True)

def iterate_order_book(csv_file_path):
    # Define the data types for each column
    dtype_dict = {
        'day': int,
        'timestamp': int,
        'product': str,
        'bid_price_1': np.float64,
        'bid_volume_1': np.float64,
        'bid_price_2': np.float64,
        'bid_volume_2': np.float64,
        'bid_price_3': np.float64,
        'bid_volume_3': np.float64,
        'ask_price_1': np.float64,
        'ask_volume_1': np.float64,
        'ask_price_2': np.float64,
        'ask_volume_2': np.float64,
        'ask_price_3': np.float64,
        'ask_volume_3': np.float64,
        'mid_price': np.float64,
        'profit_and_loss': np.float64,
        'trades': str,
    }
    
    # Load CSV file into a pandas DataFrame
    df = pd.read_csv(csv_file_path, na_values='', dtype=dtype_dict)
    
    # Create an empty DataFrame to store logging data
    logging_data = pd.DataFrame(columns=df.columns)

    # Iterate over each row
    for index, row in df.iterrows():
        logging_data = process_order_book_row(row, logging_data)

    return logging_data

# "C:\Users\Avinash\Desktop\Prosperity\logs\ret_logs\round1\merged_data_day_0.csv"
csv_file_path = "C:/Users/Avinash/Desktop/Prosperity/logs/ret_logs/round1/merged_data_day_0.csv"  # Change this to your CSV file path
logging_file = "C:/Users/Avinash/Desktop/Prosperity/logs/ret_logs/round1/prices_round_1_day_0_mod.csv"  # Change this to your logging file path
logging_data = iterate_order_book(csv_file_path)
logging_data.to_csv(logging_file, index=False) 



