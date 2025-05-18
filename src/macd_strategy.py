import pandas as pd
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MACDStrategy:
    """
    Class to calculate MACD and simulate trades based on EMA entry signals.
    """
    def __init__(
        self,
        data: pd.DataFrame,
        fast_period: int = 55,
        slow_period: int = 89,
        signal_period: int = 8,
        pip_size: float = 0.0001
    ):
        """
        Initialize with DataFrame including 'Close' and raw EMA signals.

        Adds internal columns:
          - entry_signal (only on entry rows)
          - exit_signal (only on exit rows)
          - in_position (boolean)

        Args:
            data (pd.DataFrame): Market data with raw Signal_{slow_period}EMA.
        """
        self.raw_sig = f'Signal_{slow_period}EMA'
        if 'Close' not in data.columns or self.raw_sig not in data.columns:
            raise ValueError("Input DataFrame must contain 'Close' and raw EMA signal column.")

        self.df = data.copy().reset_index(drop=True)
        self.fast = fast_period
        self.slow = slow_period
        self.signal = signal_period
        self.pip_size = pip_size

        # initialize columns
        self.df['entry_signal'] = pd.NA
        self.df['exit_signal'] = pd.NA
        self.df['in_position'] = False
        self.df['profit'] = pd.NA
        self.df['exit_type'] = pd.NA
        self.df['trade_id'] = pd.NA
        self.df['MACD'] = pd.NA
        self.df['MACD_signal'] = pd.NA
        self.df['MACD_hist'] = pd.NA

    def calculate_macd(self) -> None:
        """
        Calculate MACD, signal line, and histogram.
        """
        fast_ema = self.df['Close'].ewm(span=self.fast, adjust=False).mean()
        slow_ema = self.df['Close'].ewm(span=self.slow, adjust=False).mean()
        macd = fast_ema - slow_ema
        signal = macd.ewm(span=self.signal, adjust=False).mean()
        hist = macd - signal

        self.df['MACD'] = macd
        self.df['MACD_signal'] = signal
        self.df['MACD_hist'] = hist
        logging.info("MACD series calculated.")

    def simulate_trades(self) -> None:
        """
        Simulate trades using raw EMA signals and MACD for exit.
        Populates entry_signal, exit_signal, in_position, profit, exit_type, trade_id.
        """
        df = self.df
        trade_counter = 0
        in_trade = False
        entry_price = None
        position = None

        for idx in df.index:
            raw = df.at[idx, self.raw_sig]
            close = df.at[idx, 'Close']

            # detect entry if not in trade and raw signal is valid
            if not in_trade and pd.notna(raw) and raw in ('long', 'short'):
                in_trade = True
                position = raw
                entry_price = close
                trade_counter += 1
                df.at[idx, 'entry_signal'] = position
                df.at[idx, 'in_position'] = True
                df.at[idx, 'trade_id'] = trade_counter
                logging.info(f"Entry {position} at idx {idx}, price {entry_price}")
                continue

            # if in trade, check exit conditions
            if in_trade:
                df.at[idx, 'in_position'] = True
                df.at[idx, 'trade_id'] = trade_counter

                # forced exit on opposite raw EMA signal
                if pd.notna(raw) and raw in ('long', 'short') and raw != position:
                    profit = ((close - entry_price) if position=='long' else (entry_price - close)) / self.pip_size
                    df.at[idx, 'exit_signal'] = f"exit_{position}"
                    df.at[idx, 'exit_type'] = 'ema_cross'
                    df.at[idx, 'profit'] = round(profit,5)
                    in_trade = False
                    logging.info(f"Exit {position} at idx {idx} via EMA cross, profit {profit}")
                    continue

                # MACD exit
                hist = df.at[idx, 'MACD_hist']
                if (position=='long' and hist<0) or (position=='short' and hist>0):
                    profit = ((close - entry_price) if position=='long' else (entry_price - close)) / self.pip_size
                    df.at[idx, 'exit_signal'] = f"exit_{position}"
                    df.at[idx, 'exit_type'] = 'macd'
                    df.at[idx, 'profit'] = round(profit,5)
                    in_trade = False
                    logging.info(f"Exit {position} at idx {idx} via MACD, profit {profit}")
                    continue

        # cleanup: ensure in_position correctly reflects open trades
        df['in_position'] = df['trade_id'].notna() & df['exit_signal'].isna()
        logging.info("Trade simulation finished.")
        df['in_position'] = df['trade_id'].notna() & df['exit_signal'].isna() | df['exit_signal'].notna()
        logging.info("Trade simulation finished.")

    def get_dataframe(self) -> pd.DataFrame:
        return self.df


if __name__ == '__main__':
    from ema_signals import process_ema_signals
    input_path = '/Users/puneetanand/Documents/projects/max-algos/tmp_data/_EURUSD_2025-02-15 16:19:59.001851_raw_data.csv'
    output_path = '/Users/puneetanand/Documents/projects/max-algos/tmp_data/_EURUSD_output_signals_macd.csv'
    df_raw = pd.read_csv(input_path)
    df_ema = process_ema_signals(df_raw)
    strat = MACDStrategy(df_ema)
    strat.calculate_macd()
    strat.simulate_trades()
    df = strat.get_dataframe()
    print(df.tail(10))
    df.to_csv(output_path, index=False)
    logging.info("Saved results.")





# if __name__ == '__main__':
#     # Example usage: adjust paths as needed
#     from ema_signals import process_ema_signals

#     input_path = '/Users/puneetanand/Documents/projects/max-algos/tmp_data/_EURUSD_2025-02-15 16:19:59.001851_raw_data.csv'
#     output_path = '/Users/puneetanand/Documents/projects/max-algos/tmp_data/_EURUSD_output_signals_macd.csv'

#     # load and compute EMA signals
#     df_raw = pd.read_csv(input_path)
#     df_ema = process_ema_signals(df_raw)

#     # initialize and compute MACD strategy
#     strategy = MACDStrategy(df_ema)
#     strategy.calculate_macd()
#     strategy.simulate_trades()
#     result_df = strategy.get_dataframe()

#     # display and save
#     print(result_df.tail(10))
#     result_df.to_csv(output_path, index=False)
#     logging.info(f"Saved MACD strategy results to {output_path}.")
