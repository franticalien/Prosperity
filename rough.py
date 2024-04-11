import os
import pandas as pd
from simul import * 

fname = "round1"

# Folder path containing the files
folder_path = "C:/Users/Avinash/Desktop/Prosperity/logs/" + fname

import csv
from collections import defaultdict

def load_csv(file_path):
    data = []
    with open(file_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip header
        for row in reader:
            data.append(row)
    return data

def merge_order_book_trades(order_book_data, trades_data):
    merged_data = defaultdict(list)
    
    # Merge order book data
    for order_entry in order_book_data:
        day, timestamp, product, *rest = order_entry
        merged_data[(timestamp, product)].append([day, timestamp, product, *rest, []])

    
    # Merge trades data
    for trade_entry in trades_data:
        timestamp, buyer, seller, symbol, currency, price, quantity = trade_entry
        merged_data[(timestamp, symbol)][0][-1].append((price,quantity))
    
    return merged_data

def write_merged_data_to_csv(merged_data, output_file):
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['day', 'timestamp', 'product', 'bid_price_1', 'bid_volume_1', 'bid_price_2', 'bid_volume_2', 'bid_price_3', 'bid_volume_3', 'ask_price_1', 'ask_volume_1', 'ask_price_2', 'ask_volume_2', 'ask_price_3', 'ask_volume_3', 'mid_price', 'profit_and_loss', 'trades'])
        # writer.writerow(['day','timestamp', 'symbol', 'bid_price_1', 'Bid Volume 1', 'Bid Price 2', 'Bid Volume 2', 'Bid Price 3', 'Bid Volume 3', 'Ask Price 1', 'Ask Volume 1', 'Ask Price 2', 'Ask Volume 2', 'Ask Price 3', 'Ask Volume 3', 'Mid Price', 'Profit and Loss', 'Trades'])
        for key, value in merged_data.items():
            for entry in value:
                writer.writerow(entry)

# Loop over all pairs of files
for i in range(-2,1):
    order_book_file = folder_path + "/" + f'prices_round_1_day_{i}.csv'
    trades_file = folder_path + "/" + f'trades_round_1_day_{i}_nn.csv'
    output_file = folder_path + "/" + f'merged_data_day_{i}.csv'

    # Load order book and trades data from CSV files
    order_book_data = load_csv(order_book_file)
    trades_data = load_csv(trades_file)

    # Merge data
    merged_data = merge_order_book_trades(order_book_data, trades_data)

    # Write merged data to a new CSV file
    write_merged_data_to_csv(merged_data, output_file)



directory_path = os.path.join("C:/Users/Avinash/Desktop/Prosperity/logs/ret_logs", fname)
if not os.path.exists(directory_path):
    os.mkdir(directory_path)

# Iterate over all files in the folder
for filename in os.listdir(folder_path):
    if filename.startswith("merged_data") and filename.endswith(".csv"):  # Check if the file is a CSV file
        file_path = os.path.join(folder_path, filename)
        
        # Read the content of the file, replace semicolons with commas, and store it in a variable
        with open(file_path, 'r') as file:
            file_content = file.read().replace(';', ',')

        # Write the modified content back to the file
        with open(file_path, 'w') as file:
            file.write(file_content)

        # Read the CSV file
        df = pd.read_csv(file_path)

        # Convert the timestamp column to integer
        df['timestamp'] = df['timestamp'].astype(int)

        # Group the data by symbol
        grouped = df.groupby("product")

        # Initialize a list to store the results
        results = []

        rets = [5,10,20,100,200]

        # Iterate over each symbol group
        for name, group in grouped:
            # Sort the group by timestamp
            group = group.sort_values('timestamp')

            for d in rets:   
                # Calculate mid value after 200 units of time
                group['mid_after'] = group['mid_price'].shift(-d)
                
                # Compute the difference between mid_after_200 and mid
                group['ret_' + str(d)] = (group['mid_after'] - group['mid_price'])/group['mid_price']

            group.drop(columns=['mid_after'], inplace=True)
            
            # Drop NaN values (due to shifting)
            # group.dropna(inplace=True)
            
            # Append the results to the list
            results.append(group)

        # Concatenate the results
        final_result = pd.concat(results)

        # Write the result to a new CSV file
        csv_file_path = 'logs/ret_logs/' + fname + "/" + filename
        final_result.to_csv(csv_file_path, index=False)

        logging_file = 'logs/ret_logs/' + fname + "/" + filename.rstrip(".csv") + "_mod.csv"
        logging_data = iterate_order_book(csv_file_path)
        logging_data.to_csv(logging_file, index=False) 
        print("lund")




