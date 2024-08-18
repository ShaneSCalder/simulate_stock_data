import os
import pandas as pd
import numpy as np
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the stocks and parameters
stocks = ['AAPL', 'GOOGL', 'NVDA', 'MSFT']
simulation_days = 60  # Simulate for 60 days
intervals_per_day = 13  # 13 intervals of 30 minutes during typical trading hours
num_simulations = 1000  # Number of simulations to generate for each stock
data_dir = 'data'

# Function to load analysis data
def load_analysis(stock, analysis_path):
    try:
        with open(analysis_path, 'r') as f:
            analysis = json.load(f)
        logging.info(f"Loaded analysis for {stock} from {analysis_path}")
        return analysis
    except Exception as e:
        logging.error(f"Error loading analysis for {stock}: {e}")
        return None

# Function to generate bounded random values based on analysis
def generate_random_value(mean, std_dev, min_value, max_value):
    value = np.random.normal(mean, std_dev)
    return max(min(value, max_value), min_value)

# Function to simulate stock data based on historical analysis
def simulate_stock_data(stock_data, analysis, simulation_days, intervals_per_day):
    try:
        # Initialize lists to hold simulated OHLCV data
        simulated_open = []
        simulated_high = []
        simulated_low = []
        simulated_close = []
        simulated_volume = []

        # Start with the last known price and volume
        last_close = stock_data['Close'].iloc[-1]
        last_volume = stock_data['Volume'].iloc[-1]

        for day in range(simulation_days):
            for interval in range(intervals_per_day):
                # Generate the next close price within the 5th and 95th percentile bounds
                next_return = generate_random_value(
                    analysis['30_min_intervals']['mean_change'],
                    analysis['30_min_intervals']['std_dev_change'],
                    analysis['30_min_intervals']['percentiles']['extremes']['5th_percentile'],
                    analysis['30_min_intervals']['percentiles']['extremes']['95th_percentile']
                )
                next_close = last_close * (1 + next_return)
                
                # Generate the next open price (small random variation from last close)
                next_open = last_close * (1 + generate_random_value(0, 0.001, -0.001, 0.001))
                
                # Generate high and low prices (fluctuate within a range around open and close)
                next_high = max(next_open, next_close) * (1 + np.random.uniform(0, 0.001))
                next_low = min(next_open, next_close) * (1 - np.random.uniform(0, 0.001))
                
                # Generate the next volume using random normal variation
                next_volume = max(0, np.random.normal(stock_data['Volume'].mean(), stock_data['Volume'].std()))  # Ensure volume is not negative
                
                # Append the generated data to the lists
                simulated_open.append(next_open)
                simulated_high.append(next_high)
                simulated_low.append(next_low)
                simulated_close.append(next_close)
                simulated_volume.append(next_volume)
                
                # Update the last close price for the next interval
                last_close = next_close
                last_volume = next_volume

        # Create a DataFrame to store the simulated OHLCV data
        simulation_df = pd.DataFrame({
            'Open': simulated_open,
            'High': simulated_high,
            'Low': simulated_low,
            'Close': simulated_close,
            'Volume': simulated_volume
        })

        return simulation_df
    except Exception as e:
        logging.error(f"Error during simulation: {e}")
        return None

# Main loop to load analysis, perform simulations, and save results
for stock in stocks:
    try:
        stock_dir = os.path.join(data_dir, stock)
        analysis_path = os.path.join(stock_dir, f'{stock}_analysis.json')
        simulation_dir = os.path.join(stock_dir, 'simulations')
        os.makedirs(simulation_dir, exist_ok=True)

        # Load the stock's historical data
        csv_path = os.path.join(stock_dir, f'{stock}_30min.csv')
        stock_data = pd.read_csv(csv_path, index_col=0, parse_dates=True)

        # Load the analysis data
        analysis = load_analysis(stock, analysis_path)
        
        if analysis is not None and not stock_data.empty:
            # Perform Monte Carlo simulations
            for i in range(num_simulations):  # Perform 1000 simulations
                simulation_df = simulate_stock_data(stock_data, analysis, simulation_days, intervals_per_day)
                
                if simulation_df is not None:
                    # Number the intervals sequentially (e.g., Day 1, Interval 1)
                    simulation_df.index.name = 'Interval'
                    
                    # Save the simulation to a CSV file
                    simulation_csv_path = os.path.join(simulation_dir, f'{stock}_simulation_{i+1}.csv')
                    simulation_df.to_csv(simulation_csv_path)
                    
                    logging.info(f'Simulation {i+1} completed for {stock}')
        else:
            logging.error(f"Missing analysis or stock data for {stock}. Skipping simulations.")
    
    except Exception as e:
        logging.error(f"Error processing {stock}: {e}")

