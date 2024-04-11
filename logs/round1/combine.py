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
        writer.writerow(['Day','Timestamp', 'Symbol', 'Bid Price 1', 'Bid Volume 1', 'Bid Price 2', 'Bid Volume 2', 'Bid Price 3', 'Bid Volume 3', 'Ask Price 1', 'Ask Volume 1', 'Ask Price 2', 'Ask Volume 2', 'Ask Price 3', 'Ask Volume 3', 'Mid Price', 'Profit and Loss', 'Trades'])
        for key, value in merged_data.items():
            for entry in value:
                writer.writerow(entry)

# Loop over all pairs of files
for i in range(-2,1):
    order_book_file = f'prices_round_1_day_{i}.csv'
    trades_file = f'trades_round_1_day_{i}_nn.csv'
    output_file = f'merged_data_day_{i}.csv'

    # Load order book and trades data from CSV files
    order_book_data = load_csv(order_book_file)
    trades_data = load_csv(trades_file)

    # Merge data
    merged_data = merge_order_book_trades(order_book_data, trades_data)

    # Write merged data to a new CSV file
    write_merged_data_to_csv(merged_data, output_file)
