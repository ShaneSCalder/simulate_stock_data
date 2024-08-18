import os
import pandas as pd
import numpy as np
import json
import shutil
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the stocks and parameters
stocks = ['AAPL', 'GOOGL', 'NVDA', 'MSFT']
data_dir = 'data'
threshold_deviation_factor = 2  # Threshold for filtering simulations based on standard deviation deviation

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

# Function to evaluate a simulation against historical analysis
def evaluate_simulation(simulation_df, analysis):
    try:
        # Calculate returns for 30-minute intervals in the simulation
        simulation_df['Returns'] = simulation_df['Close'].pct_change()

        # Compare mean and standard deviation of returns with historical analysis
        sim_mean_change = simulation_df['Returns'].mean()
        sim_std_dev_change = simulation_df['Returns'].std()

        hist_mean_change = analysis['30_min_intervals']['mean_change']
        hist_std_dev_change = analysis['30_min_intervals']['std_dev_change']

        # Apply threshold check based on deviation from historical mean and standard deviation
        if (abs(sim_mean_change - hist_mean_change) <= threshold_deviation_factor * hist_std_dev_change) and \
           (abs(sim_std_dev_change - hist_std_dev_change) <= threshold_deviation_factor * hist_std_dev_change):
            return True
        else:
            return False

    except Exception as e:
        logging.error(f"Error evaluating simulation: {e}")
        return False

# Main loop to evaluate simulations and select valid ones
for stock in stocks:
    try:
        stock_dir = os.path.join(data_dir, stock)
        analysis_path = os.path.join(stock_dir, f'{stock}_analysis.json')
        simulation_dir = os.path.join(stock_dir, 'simulations')
        selected_simulation_dir = os.path.join(stock_dir, 'simulated')
        
        # Load analysis data
        analysis = load_analysis(stock, analysis_path)
        
        if analysis is not None and os.path.exists(simulation_dir):
            os.makedirs(selected_simulation_dir, exist_ok=True)
            valid_simulations = []
            
            # Loop through simulation files
            for simulation_file in os.listdir(simulation_dir):
                simulation_path = os.path.join(simulation_dir, simulation_file)
                
                if simulation_file.endswith('.csv'):
                    simulation_df = pd.read_csv(simulation_path, index_col=0, parse_dates=True)
                    
                    # Evaluate the simulation against historical data
                    if evaluate_simulation(simulation_df, analysis):
                        valid_simulations.append(simulation_file)
                    else:
                        logging.info(f"Simulation {simulation_file} did not meet criteria and will be excluded.")
            
            # Copy valid simulations to the 'simulated' directory and renumber them
            for i, simulation_file in enumerate(valid_simulations, start=1):
                new_filename = f'{stock}_simulation_{i}.csv'
                shutil.copy(os.path.join(simulation_dir, simulation_file), os.path.join(selected_simulation_dir, new_filename))
                logging.info(f"Simulation {simulation_file} selected and saved as {new_filename}")

            logging.info(f"Selected {len(valid_simulations)} valid simulations for {stock}")
        
        else:
            logging.error(f"Missing analysis or simulation data for {stock}. Skipping.")
    
    except Exception as e:
        logging.error(f"Error processing {stock}: {e}")



