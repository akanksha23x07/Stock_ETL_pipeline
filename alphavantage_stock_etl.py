import os
import sys
import requests
import logging
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values



# ----- Logger Setup -----
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(name)s: %(lineno)d: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),                  
        logging.FileHandler("extraction.log", mode='a')  
    ]
)
logger = logging.getLogger(__name__)

# ---- Load environment variables ----
load_dotenv()
alphavantege_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
db_name="airflow_db"
db_host="postgresdb"
db_user="airflow"
db_pass="airflow_password"
db_port="5432"

""" Database Connection """
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_pass
)

def fetch_stock_metadata(symbol_dict: dict) -> dict:
    """
    Extracts stock metadata from a provided dictionary and returns it as a structured dictionary.

    This function is used to parse the response from a stock metadata API call.
    It extracts key metadata fields and includes timestamp fields (`created_at`, `updated_at`)
    to track data loading time.

    Parameters:
    -----------
    symbol_dict : dict
        Dictionary containing stock metadata, usually from an API response.

    Returns:
    --------
    dict
        A dictionary with standardized keys including:
        - 'Symbol'
        - 'created_at', 'updated_at'
        - Metadata fields like 'AssetType', 'Sector', 'MarketCapitalization', etc.
        If fields are missing from the input, they are filled with "NA" or `None` in case of errors.
    """
    
    logger.info("fetch_stock_metadata execution started...")
    
    try:
    
        fields = [
            'Description', 'AssetType', 'Name', 'Exchange', 'Currency', 'Country', 'Sector',
            'Industry', 'Address', 'OfficialSite', 'FiscalYearEnd', 'LatestQuarter',
            'MarketCapitalization', 'EBITDA', 'PERatio', 'PEGRatio', 'BookValue',
            'DividendPerShare', 'DividendYield', 'RevenuePerShareTTM', 'ProfitMargin',
            'OperatingMarginTTM', 'ReturnOnAssetsTTM', 'ReturnOnEquityTTM', 'RevenueTTM',
            'GrossProfitTTM', 'QuarterlyEarningsGrowthYOY', 'QuarterlyRevenueGrowthYOY',
            'PriceToBookRatio', '50DayMovingAverage', '200DayMovingAverage', 'SharesOutstanding'
        ]
        
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            data = {
                'Symbol': symbol_dict['Symbol'],
                'created_at': now,
                'updated_at': now,
            }

            for field in fields:
                data[field] = symbol_dict.get(field, "NA")
                
        except Exception as e:
            logger.info(f"Could not fetch symbol overview due to: {e}")
            data = {'Symbol': symbol_dict.get('Symbol', None), 'created_at': now, 'updated_at': now,}
            for field in fields:
                data[field] = None
    except Exception as e:
        logger.info(f"Failed to extract data due {e}")

    logger.info("fetch_stock_metadata Execution finished!")
    logger.info(data)
    return data

def fetch_daily_stock_data(daily_stock_dict: dict) -> pd.DataFrame:
    """
    Extracts daily stock data from a nested dictionary structure (typically returned by a stock API)
    and converts it into a pandas DataFrame.

    Parameters:
    -----------
    daily_stock_dict : dict
        Dictionary containing daily time series stock data along with metadata.

    Returns:
    --------
    pd.DataFrame
        A DataFrame containing columns: company_symbol, created_at, date, open, high, low, close, volume.
        Each row represents a day's stock data for the given company symbol.
    """
    
    logger.info("fetch_daily_stock_data Execution started...")

    field_df = pd.DataFrame()
    try:
        stock_rows = []
        daywise_stock_map = daily_stock_dict['Time Series (Daily)']
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for day, stockinfo in daywise_stock_map.items():

            row = {
                'company_symbol': daily_stock_dict['Meta Data']['2. Symbol'],
                'created_at': created_at,
                'date': day,
                'open': stockinfo.get('1. open'),
                'high': stockinfo.get('2. high'),
                'low': stockinfo.get('3. low'),
                'close': stockinfo.get('4. close'),
                'volume': stockinfo.get('5. volume')
            }
            stock_rows.append(row)
            
        field_df = pd.DataFrame(stock_rows)

    except Exception as e:
        logger.info(f"could not fetch Daywise stock data due to: {e}")
        
    # pprint(field_df)
    # logger.info(field_df.head(5).to_string())
    logger.info("fetch_daily_stock_data Execution finished!")
    return field_df
    
def load_update_daily_stock_data_to_db(df: pd.DataFrame, conn):
    """
    Loads daily stock data into the `stock_data` table, ignoring duplicates based on (company_symbol, date).
    
    Parameters:
    - df: DataFrame containing daily stock records
    - conn: Active PostgreSQL database connection
    """
    logger.info("load_update_daily_stock_data_to_db Execution started...")
    
    try:

        insert_query = """
        INSERT INTO stock_data (
            company_symbol, created_at, date, open, high, low, close, volume
        ) Values %s 
        ON CONFLICT (company_symbol, date) DO NOTHING;
        """
        
        values = [
            (
                row['company_symbol'],
                row['created_at'],
                row['date'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            )
            for _, row in df.iterrows()
        ]
        
        with conn.cursor() as cur:
            execute_values(cur, insert_query, values)
        conn.commit()
        
    except Exception as e:
        logger.info(f"Failed to load daily_stock_data due to {e}")
    logger.info("load_update_daily_stock_data_to_db execution finished!")
            
def load_update_stock_metadata_to_db(stock_metadata_dict: dict, symbol, conn):
    """
    Inserts or updates stock metadata in the `stock_metadata` table.
    Updates only if values have changed, preserving existing data.

    Parameters:
    - stock_metadata_dict: Dictionary with stock metadata
    - symbol: Stock symbol (cannot be None)
    - conn: Active PostgreSQL database connection
    """
    if not symbol:
        logger.warning("Symbol is None or empty — skipping DB update.")
        return

    logger.info("load_update_stock_metadata_to_db Execution started...")

    fields = [
        'Symbol', 'Description', 'AssetType', 'Name', 'Exchange', 'Currency',
        'Country', 'Sector', 'Industry', 'Address', 'OfficialSite',
        'FiscalYearEnd', 'LatestQuarter', 'MarketCapitalization', 'EBITDA',
        'PERatio', 'PEGRatio', 'BookValue', 'DividendPerShare',
        'DividendYield', 'RevenuePerShareTTM', 'ProfitMargin',
        'OperatingMarginTTM', 'ReturnOnAssetsTTM', 'ReturnOnEquityTTM',
        'RevenueTTM', 'GrossProfitTTM', 'QuarterlyEarningsGrowthYOY',
        'QuarterlyRevenueGrowthYOY', 'PriceToBookRatio',
        '50DayMovingAverage', '200DayMovingAverage', 'SharesOutstanding'
    ]

    # Clean values
    cleaned_values = [
        None if stock_metadata_dict.get(k) in ["None", "", None, "-", "_", "NA", "N/A"] 
        else stock_metadata_dict.get(k)
        for k in fields[1:]  # Skip Symbol
    ]

    # If all fields except Symbol are None → bad/empty API data → skip update
    if all(v is None for v in cleaned_values):
        logger.warning(f"No valid data returned for {symbol} — skipping DB update.")
        return

    # Prepend Symbol to values
    values = [symbol] + cleaned_values

    # Rename moving averages to match DB
    fields[fields.index("50DayMovingAverage")] = "MovingAverage50Day"
    fields[fields.index("200DayMovingAverage")] = "MovingAverage200Day"

    update_fields = [f"{f} = EXCLUDED.{f}" for f in fields if f != "Symbol"]
    change_check = [f"stock_metadata.{f} IS DISTINCT FROM EXCLUDED.{f}" for f in fields if f != "Symbol"]

    insert_query = f"""
    INSERT INTO stock_metadata ({", ".join(fields)}, created_at, updated_at)
    VALUES ({", ".join(["%s"] * len(fields))}, NOW(), NOW())
    ON CONFLICT (Symbol) DO UPDATE
    SET {", ".join(update_fields)}, updated_at = NOW()
    WHERE {" OR ".join(change_check)};
    """

    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, values)
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to load stock_metadata for {symbol} due to {e}")

    logger.info("load_update_stock_metadata_to_db Execution finished!")


def fetch_symbol_list():
    """
    Returns a list of stock symbols to process.
    
    If a single symbol is provided via the command line, verifies it using Alpha Vantage.
    If invalid, fetches the top 5 matching symbols. If no symbol is given, defaults to a predefined list.

    Returns:
    list: A list of valid stock symbol strings.
    """
    logger.info("fetch_symbol_list Execution started...")
    # ----- Default company list if none provided -----
    default_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "IBM"]

    # ----- Get company(symbol) names from command line -----
    if len(sys.argv) == 2:
        symbols = [sys.argv[1].upper()]
        logger.info(f"Running for symbol: {symbols[0]}")
        
        # ---- Check if Symbol Exists, if not, create a bestmatches list of 5 symbols----
        url = f'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={symbols[0]}&apikey={alphavantege_api_key}'
        r = requests.get(url)
        data = r.json()
        
        
        if symbols[0] == data['bestMatches'][0]['1. symbol']:
            logger.info(f"Provided symbol is correct, fetching data for {symbols[0]}")
        else:
            new_symbols = []
            for num in range(4):
                new_symbols.append(data['bestMatches'][num]['1. symbol'])
            symbols = new_symbols
            logger.info(f"Provided symbol {symbols[0]} is not a correct symbol, fetching data for best 5 matches: {symbols}")
        
    else:
        logger.warning("No symbol provided. Falling back to default symbol list.")
        symbols = default_symbols
    logger.info("fetch_symbol_list execution finished")
    return symbols
    
def ETL_data(symbols: list):
    """
    Extracts, transforms, and loads stock data and metadata for a list of stock symbols
    from Alpha Vantage API into the database.

    Parameters:
    symbols (list): List of stock symbol strings to process.
    """
    logger.info("ETL_data Execution started...")
    # ---- loop Through symbols and call API -----
    for symbol in symbols:
        
        symbol_url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={alphavantege_api_key}"
        symbol_data_response = requests.get(symbol_url)
            
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={alphavantege_api_key}'    
        try:
            logger.info(f"Fetching stock data for: {symbol}")
            r = requests.get(url)
            
            success_flag = r.status_code
            
            if (success_flag == 200) and (symbol_data_response.status_code == 200):
                
                # Extract
                
                stock_data = r.json()
                logger.info(f"Successfully extracted stock data for {symbol}")
                
                stock_metadata_dict = symbol_data_response.json()
                logger.info(f"Successfully extracted stock meta data for {symbol}")


                # transfer/field_mapping
                    
                """Fetching Daywise Stock Info"""
                daily_stock_data_dict = fetch_daily_stock_data(stock_data)
                
                """ Fetching symbol data"""
                data_dict = fetch_stock_metadata(stock_metadata_dict)
                
                # Load to DB
                load_update_stock_metadata_to_db(data_dict, symbol, conn)
                
                load_update_daily_stock_data_to_db(daily_stock_data_dict, conn)
                        
            else:
                logger.info(f"Something went wrong, please check success_flag: {success_flag}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error for {symbol}: {e}")
            
    logger.info("ETL_data execution finished!")
    if conn:
        conn.close()
        logger.info("Database connection closed.")        
        
        
        
def run_etl(symbols: list = None):
    """
    Main entry point to run the ETL pipeline.

    Parameters:
    symbols (list, optional): List of stock symbols to fetch. If None, will use default or CLI input.
    """
    if symbols is None:
        symbols = fetch_symbol_list()
    ETL_data(symbols)
        
if __name__ == "__main__":
    run_etl()
    
