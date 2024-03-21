import json
import csv
import pandas as pd

# Open the log file for reading
fname = 'example1'
with open('logs/downloads/' + fname + '_log.log', 'r') as log_file:
    # Skip the first line (heading)
    next(log_file)
    
    # Open a text file for writing the lambda logs
    with open('logs/temp/lambda_data/' + fname + '_lambda_log.txt', 'w') as output_file:
        # Initialize a variable to store lines belonging to the current JSON object
        current_json = ''
        
        # Iterate through each line in the log file
        for line in log_file:
            # Append the line to current_json
            current_json += line.strip()
            
            # Check if the line completes a JSON object
            if current_json.startswith('{') and current_json.endswith('}'):
                try:
                    # Try parsing the current_json as JSON
                    log_entry = json.loads(current_json)
                    # Extract the lambda log value
                    lambda_log = log_entry.get('lambdaLog', '')
                    # Write the lambda log to the output file
                    output_file.write(lambda_log + '\n')
                    # Reset current_json for the next JSON object
                    current_json = ''
                except json.JSONDecodeError:
                    # If JSON decoding fails, continue to the next line
                    continue



# Initialize an empty list to store parameters
parameters = []

# Open the log file for reading
with open('logs/temp/lambda_data/' + fname + '_lambda_log.txt', 'r') as log_file:
    # Read the first line to extract parameters
    first_line = log_file.readline().strip().strip(',')
    parts = first_line.split(', ')
    for part in parts:
        key, _ = part.split(' : ')
        parameters.append(key.strip())

    # Initialize a dictionary to store data for each parameter
    data = {param: [] for param in parameters}

    # Parse the first line
    line_data = {param: None for param in parameters}
    for part in parts:
        key, value = part.split(' : ')
        key = key.strip()
        value = value.strip()
        if key in parameters:
            line_data[key] = value
    for param in parameters:
        data[param].append(line_data[param])

    # Iterate through each remaining line in the log file
    for line in log_file:
        line = line.strip().strip(',')
        if not line:
            continue  # Skip empty lines
        
        try:
            # Split the line by comma and whitespace
            parts = line.split(', ')
            
            # Initialize a dictionary to store values for this line
            line_data = {param: None for param in parameters}
            
            # Iterate through each part in the line
            for part in parts:
                # Split the part by colon
                key, value = part.split(' : ')
                key = key.strip()
                value = value.strip()
                
                # Store the value in line_data if the key matches a parameter
                if key in parameters:
                    line_data[key] = value
            
            # Append the values for this line to the data dictionary
            for param in parameters:
                data[param].append(line_data[param])
        except Exception as e:
            print(f"Error parsing line: {line}")
            print(f"Error message: {str(e)}")

# Write the data to a CSV file
with open('logs/temp/csv/' + fname + '_csv_log.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=parameters)
    writer.writeheader()
    for i in range(len(data[parameters[0]])):
        writer.writerow({param: data[param][i] for param in parameters})

# Read the CSV file
df = pd.read_csv('logs/temp/csv/' + fname + '_csv_log.csv')

# Convert the timestamp column to integer
df['timestamp'] = df['timestamp'].astype(int)

# Group the data by symbol
grouped = df.groupby('symbol')

# Initialize a list to store the results
results = []

rets = [5,10,20,100,200]

# Iterate over each symbol group
for name, group in grouped:
    # Sort the group by timestamp
    group = group.sort_values('timestamp')

    for d in rets:   
        # Calculate mid value after 200 units of time
        group['mid_after'] = group['mid'].shift(-d)
        
        # Compute the difference between mid_after_200 and mid
        group['ret_' + str(d)] = (group['mid_after'] - group['mid'])/group['mid']

    group.drop(columns=['mid_after'], inplace=True)
    
    # Drop NaN values (due to shifting)
    # group.dropna(inplace=True)
    
    # Append the results to the list
    results.append(group)

# Concatenate the results
final_result = pd.concat(results)

# Write the result to a new CSV file
final_result.to_csv('logs/ret_logs/' + fname + '_final.csv', index=False)
