import os
import pandas as pd
import numpy as np
import yfinance as yf
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the stocks and parameters
stocks = ['AAPL', 'GOOGL', 'NVDA', 'MSFT']
interval = '30m'  # Interval of the data (30-minute intervals)
data_dir = 'data'

# Function to fetch historical data and save to CSV
def fetch_historical_data(stock, csv_path):
    try:
        if not os.path.exists(csv_path):
            logging.info(f"Fetching data for {stock} from Yahoo Finance...")
            ticker = yf.Ticker(stock)
            data = ticker.history(period='60d', interval=interval)  # Limited to last 60 days
            if not data.empty:
                data.to_csv(csv_path)
                logging.info(f"Data for {stock} saved to {csv_path}")
            else:
                logging.error(f"No data found for {stock}. Skipping...")
        else:
            logging.info(f"Data for {stock} already exists at {csv_path}")
    except Exception as e:
        logging.error(f"Error fetching data for {stock}: {e}")

# Function to load stock data from CSV
def load_stock_data(csv_path):
    try:
        logging.info(f"Loading stock data from {csv_path}")
        stock_data = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        logging.info(f"Successfully loaded stock data from {csv_path}")
        return stock_data
    except Exception as e:
        logging.error(f"Error loading stock data from {csv_path}: {e}")
        return None

# Function to analyze historical data and extract statistical parameters
def analyze_stock_data(stock_data):
    analysis = {}

    try:
        # Calculate returns for 30-minute intervals
        stock_data['Returns'] = stock_data['Close'].pct_change()

        # 30-minute interval statistics
        analysis['30_min_intervals'] = {
            'mean_change': stock_data['Returns'].mean(),
            'std_dev_change': stock_data['Returns'].std(),
            'percentiles': {
                '10th': stock_data['Returns'].quantile(0.1),
                '90th': stock_data['Returns'].quantile(0.9),
                'extremes': {
                    '5th_percentile': stock_data['Returns'].quantile(0.05),
                    '95th_percentile': stock_data['Returns'].quantile(0.95)
                }
            }
        }

        # Daily statistics
        daily_data = stock_data['Close'].resample('D').last().pct_change()
        analysis['daily_changes'] = {
            'mean_change': daily_data.mean(),
            'std_dev_change': daily_data.std(),
            'percentiles': {
                '10th': daily_data.quantile(0.1),
                '90th': daily_data.quantile(0.9),
                'extremes': {
                    '5th_percentile': daily_data.quantile(0.05),
                    '95th_percentile': daily_data.quantile(0.95)
                }
            }
        }

        # Weekly statistics
        weekly_data = stock_data['Close'].resample('W').last().pct_change()
        analysis['weekly_changes'] = {
            'mean_change': weekly_data.mean(),
            'std_dev_change': weekly_data.std(),
            'percentiles': {
                '10th': weekly_data.quantile(0.1),
                '90th': weekly_data.quantile(0.9),
                'extremes': {
                    '5th_percentile': weekly_data.quantile(0.05),
                    '95th_percentile': weekly_data.quantile(0.95)
                }
            }
        }

        logging.info(f"Analysis completed for stock data")
        return analysis

    except Exception as e:
        logging.error(f"Error analyzing stock data: {e}")
        return None

# Function to save the analysis to a JSON file
def save_analysis(stock, analysis, analysis_path):
    try:
        with open(analysis_path, 'w') as f:
            json.dump(analysis, f, indent=4)
        logging.info(f"Analysis for {stock} saved to {analysis_path}")
    except Exception as e:
        logging.error(f"Error saving analysis for {stock}: {e}")

# Main loop to fetch data, perform analysis, and save results
for stock in stocks:
    try:
        stock_dir = os.path.join(data_dir, stock)
        os.makedirs(stock_dir, exist_ok=True)

        csv_path = os.path.join(stock_dir, f'{stock}_30min.csv')
        analysis_path = os.path.join(stock_dir, f'{stock}_analysis.json')
        
        # Fetch historical data if not present
        fetch_historical_data(stock, csv_path)
        
        # Load the stock's historical data
        stock_data = load_stock_data(csv_path)
        
        if stock_data is not None and not stock_data.empty:
            # Perform analysis on the stock data
            analysis = analyze_stock_data(stock_data)
            
            if analysis is not None:
                # Save the analysis to a JSON file
                save_analysis(stock, analysis, analysis_path)
        else:
            logging.error(f"Historical data for {stock} is missing or corrupted. Skipping analysis.")
    
    except Exception as e:
        logging.error(f"Error processing {stock}: {e}")















