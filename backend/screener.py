import pandas as pd
import numpy as np
import re
from typing import Optional

# Cache the data to avoid reading the CSV file every time
_data = None

def get_stock_data() -> pd.DataFrame:
    """Read and cache stock data from CSV file if not already cached."""
    global _data
    if _data is not None:
        return _data

    try:
        # Read the CSV file from the absolute path
        csv_path = '/Users/rohanraval/Desktop/HP/stock_data.csv'
        _data = pd.read_csv(csv_path)
        # Set the Ticker column as the index
        _data.set_index('Ticker', inplace=True)
        return _data
    except Exception as e:
        raise Exception(f"Error reading stock data from CSV: {str(e)}")

def get_regex_parameters() -> str:
    """Get the regex pattern for valid parameters."""
    data = get_stock_data()
    regex_parameters = '('
    for idx in data.columns:
        regex_parameters += idx+'|' 
    regex_parameters = regex_parameters[:-1]+')'
    return regex_parameters

def screen_stocks(query: str) -> pd.DataFrame:
    """
    Screen stocks based on the provided query string.
    
    Args:
        query (str): The screening query string
    
    Returns:
        pd.DataFrame: Filtered DataFrame containing matching stocks
    """
    if not query or not query.strip():
        return pd.DataFrame()

    data = get_stock_data()
    new_data = data.copy()
    regex_parameters = get_regex_parameters()
    
    try:
        # Split by AND and process each condition
        conditions = [cond.strip() for cond in query.split('AND') if cond.strip()]
        print(f"Processing conditions: {conditions}")  # Debug log
        
        for condition in conditions:
            print(f"Processing condition: {condition}")  # Debug log
            
            # Check for arithmetic operations
            operation_search = re.search(r'[-+*]', condition)
            
            if operation_search is None:
                parameter = re.search(rf'{regex_parameters}', condition)
                operator = re.search(r'[><=]+', condition)
                target = re.search(r'[><=]+\s*([-]?\d+\.?\d*)', condition)

                if not all([parameter, operator, target]):
                    continue

                query_str = f"`{parameter.group(0)}` {operator.group(0)} {target.group(1)}"
                new_data = new_data.query(query_str)
            else:
                re_matcher = re.search(
                    rf'({regex_parameters})\s*([\/*+-])\s*({regex_parameters})\s*([><=]+)\s*([-]?\d+\.?\d*)',
                    condition
                )
                
                if not re_matcher:
                    continue

                parameter1 = re_matcher.group(1)
                operator1 = re_matcher.group(2)
                parameter2 = re_matcher.group(3)
                operator2 = re_matcher.group(4)
                target = re_matcher.group(5)

                operator_map1 = {
                    '/': lambda x, y: x / y,
                    '*': lambda x, y: x * y,
                    '+': lambda x, y: x + y,
                    '-': lambda x, y: x - y
                }

                operator_map2 = {
                    '>': lambda x, y: x > y,
                    '<': lambda x, y: x < y,
                    '>=': lambda x, y: x >= y,
                    '<=': lambda x, y: x <= y,
                    '=': lambda x, y: x == y,
                    '==': lambda x, y: x == y,
                    '!=': lambda x, y: x != y
                }

                try:
                    ser = operator_map1[operator1](new_data[parameter1], new_data[parameter2])
                    bool_ser = operator_map2[operator2](ser, float(target))
                    new_data = new_data.loc[bool_ser]
                except Exception as e:
                    print(f"Error processing complex condition: {str(e)}")
                    continue

        # Define the exact column order we want
        columns_order = ['Ticker', 'Market Capitalization', 'Total Debt', 'Total Revenue',
                        'Free Cashflow 3years %', 'Free Cashflow 4years %', 'Free Cashflow',
                        'Operating Cashflow 3years %', 'Operating Cashflow 4years %', 'Operating Cashflow',
                        'Investing Cashflow 3years %', 'Investing Cashflow 4years %', 'Investing Cashflow',
                        'Financing Cashflow 3years %', 'Financing Cashflow 4years %', 'Financing Cashflow',
                        'P/E Ratio', 'Forward P/E Ratio', 'P/B Ratio', 'Debt-to-Equity Ratio',
                        'Current Ratio', 'Quick Ratio', 'ROE', 'ROA', 'Profit Margin',
                        'Operating Margin', 'Gross Margin']
        
        # Reset index and reorder columns
        result = new_data.reset_index()
        return result[columns_order]

    except Exception as e:
        print(f"Error in screen_stocks: {str(e)}")
        print("Full error:", e)
        return pd.DataFrame() 