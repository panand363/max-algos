# This script calculates Exponential Moving Averages (EMA) and generates trading signals
# based on the crossing of the Close price with the EMA.
# It uses the pandas library for data manipulation and logging for tracking progress.



import logging
import pandas as pd


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EMASignal:
    """
    A class to calculate EMA signals and save results.
    """
    def __init__(self, data: pd.DataFrame, ema_period: int = 89):
        """
        Initialize with a DataFrame containing Open, High, Low, Close columns.

        Args:
            data (pd.DataFrame): Input market data.
            ema_period (int): Period for the EMA calculation.

        Raises:
            ValueError: If the required columns are missing.
        """
        required_cols = {'Open', 'High', 'Low', 'Close'}
        if not required_cols.issubset(data.columns):
            missing = required_cols - set(data.columns)
            raise ValueError(f"Missing required columns: {missing}")

        self.df = data.copy()
        self.ema_period = ema_period

    def calculate_ema(self) -> None:
        """
        Calculate the Exponential Moving Average and add it as 'EMA_{period}'.
        """
        ema_col = f'EMA_{self.ema_period}'
        self.df[ema_col] = self.df['Close'].ewm(span=self.ema_period, adjust=False).mean()
        logging.info(f"Calculated {ema_col}.")

    def generate_signals(self) -> None:
        """
        Generate long/short signals when Close crosses above/below the EMA.
        Populates 'Signal_{period}EMA' column.
        """
        ema_col = f'EMA_{self.ema_period}'
        sig_col = f'Signal_{self.ema_period}EMA'

        if ema_col not in self.df.columns:
            raise RuntimeError(f"{ema_col} not found. Call calculate_ema() first.")

        close = self.df['Close']
        ema = self.df[ema_col]
        prev_close = close.shift(1)
        prev_ema = ema.shift(1)

        cross_above = (close > ema) & (prev_close <= prev_ema)
        cross_below = (close < ema) & (prev_close >= prev_ema)

        self.df[sig_col] = pd.NA
        self.df.loc[cross_above, sig_col] = 'long'
        self.df.loc[cross_below, sig_col] = 'short'
        logging.info(f"Generated signals in column {sig_col}.")

    def get_dataframe(self) -> pd.DataFrame:
        """
        Return the processed DataFrame.
        """
        return self.df

    def save_to_csv(self, path: str) -> None:
        """
        Save the processed DataFrame to a CSV file.
        """
        try:
            self.df.to_csv(path, index=False)
            logging.info(f"Saved DataFrame to {path}.")
        except Exception as e:
            logging.error(f"Failed to save to CSV: {e}")
            raise


def process_ema_signals(df: pd.DataFrame, ema_period: int = 89) -> pd.DataFrame:
    """
    Helper function to compute EMA and signals on a DataFrame.
    """
    processor = EMASignal(df, ema_period)
    processor.calculate_ema()
    processor.generate_signals()
    return processor.get_dataframe()


if __name__ == '__main__':
    # Example usage: adjust file paths as needed
    input_path = '/Users/puneetanand/Documents/projects/max-algos/tmp_data/_EURUSD_2025-02-15 16:19:59.001851_raw_data.csv'
    output_path = '/Users/puneetanand/Documents/projects/max-algos/tmp_data/_EURUSD_output_signals.csv'

    try:
        df_input = pd.read_csv(input_path)
        logging.info(f"Loaded data from {input_path}.")
    except FileNotFoundError:
        logging.error(f"Input file '{input_path}' not found.")
    except Exception as e:
        logging.error(f"Error loading input CSV: {e}")
    else:
        df_result = process_ema_signals(df_input)
        print(df_result.tail(10))
        try:
            df_result.to_csv(output_path, index=False)
            logging.info(f"Saved results to {output_path}.")
        except Exception as e:
            logging.error(f"Could not save results: {e}")
