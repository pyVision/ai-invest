import os
import time
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Import the official download functions
from download_kline import download_monthly_klines, download_daily_klines


def daterange(start_date, end_date):
  for n in range(int((end_date - start_date).days) + 1):
    yield start_date + timedelta(n)

def get_months_between(start_date, end_date):
  months = []
  current = start_date.replace(day=1)
  while current <= end_date:
    months.append(current)
    current += relativedelta(months=1)
  return months

def get_existing_dates(csv_path):
  if not os.path.exists(csv_path):
    return set()
  df = pd.read_csv(csv_path, header=None)
  return set(pd.to_datetime(df[0], unit='ms').dt.date)

def download_with_backoff(download_func, rate_limit_sleep, max_backoff, *args, **kwargs):
  sleep_time = rate_limit_sleep
  while True:
    try:
      download_func(*args, **kwargs)
      return
    except Exception as e:
      # Check for HTTP 429 or rate limit in error message
      if "429" in str(e) or "rate limit" in str(e).lower():
        print(f"Rate limited. Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)
        sleep_time = min(sleep_time * 2, max_backoff)
      else:
        raise

def main(symbol, interval, start_date, end_date, rate_limit_sleep, max_backoff, data_dir, cksum=0):
    os.makedirs(data_dir, exist_ok=True)
    merged_csv = os.path.join(data_dir, f"{symbol}_{interval}.csv")
    existing_dates = get_existing_dates(merged_csv)


    print(f"Existing dates in {merged_csv}: {existing_dates}")
    market_type = "spot"
    # Determine num_symbols based on symbol input
    if "," in symbol:
        symbols_list = [s.strip() for s in symbol.split(",") if s.strip()]
        num_symbols = len(set(symbols_list))
    else:
        num_symbols = 1
    months = get_months_between(start_date, end_date)
    files_to_download = []



    for month in months:
        month_start = month
        month_end = (month + relativedelta(months=1)) - timedelta(days=1)
        # Check if all dates in the month are missing from existing_dates
        month_dates = set(daterange(max(month_start, start_date), min(month_end, end_date)))
        if month_start >= start_date and month_end <= end_date and not month_dates.issubset(existing_dates):
            files_to_download.append(("monthly", (month, month + relativedelta(months=1))))
        else:
            for single_date in daterange(max(month_start, start_date), min(month_end, end_date)):
                if single_date not in existing_dates:
                    files_to_download.append(("daily", (single_date, single_date + timedelta(days=1))))

    all_new_rows = []
    for freq, date in files_to_download:
        out_dir = os.path.join(data_dir, f"{symbol}_{interval}_data")
        os.makedirs(out_dir, exist_ok=True)
        print(f"Processing {freq} data for {symbol} {interval} from {date[0]} to {date[1]}...")
        try:
            if freq == "monthly":

                # 'date' here is a tuple: (month_start, month_end)
                month_start, month_end = date
                # Create a list of months between month_start and month_end (inclusive)
                months = []
                current_month = month_start
                while current_month <= (month_end - timedelta(days=1)):
                    months.append(current_month.strftime('%Y-%m'))
                    current_month += relativedelta(months=1)
                # months now contains all months in the range
                month_start = month_start.strftime('%Y-%m-%d')
                month_end = month_end.strftime('%Y-%m-%d')
                download_with_backoff(download_monthly_klines, rate_limit_sleep, max_backoff, market_type,[symbol],num_symbols, [interval], None,months,month_start, month_end, out_dir, cksum)
                

                fname = f"{symbol}-{interval}-{month_start}.csv"
                print(f"Downloading monthly data for {symbol} {interval} from {month_start} to {month_end} : {fname} : {out_dir}")
            else:
                # 'date' here is a tuple: (day_start, day_end)
                day_start, day_end = date
                day_start = day_start.strftime('%Y-%m-%d')
                day_end = day_end.strftime('%Y-%m-%d')

                print(f"Downloading daily data for {symbol} {interval} from {day_start} to {day_end}  : {out_dir}")

                download_with_backoff(download_daily_klines, rate_limit_sleep, max_backoff, market_type,[symbol],num_symbols, [interval], [day_start],day_start, day_end, out_dir, cksum)
                #day_start, day_end = date
                fname = f"{symbol}-{interval}-{day_start}.csv"

            csv_path = os.path.join(out_dir, fname)
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, header=None)
                df['date'] = pd.to_datetime(df[0], unit='ms').dt.date
                df = df[~df['date'].isin(existing_dates)] 
                all_new_rows.append(df.drop(columns=['date']))
            # Clean up
            #for f in os.listdir(out_dir):
            #    os.remove(os.path.join(out_dir, f))
            time.sleep(rate_limit_sleep)
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error processing {freq} {date}: {e}")

    if all_new_rows:
        new_data = pd.concat(all_new_rows)
        if os.path.exists(merged_csv):
            old_data = pd.read_csv(merged_csv, header=None)
            merged = pd.concat([old_data, new_data]).drop_duplicates(subset=[0])
        else:
            merged = new_data
        merged.sort_values(by=0, inplace=True)
        merged.to_csv(merged_csv, header=False, index=False)
        print(f"Updated {merged_csv} with new data.")
    else:
        print("No new data to add.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True, help="e.g. BTCUSDT")
    parser.add_argument("--interval", required=True, help="e.g. 1m, 5m, 1h")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--rate-limit-sleep", type=int, default=1, help="Initial sleep (seconds) between requests")
    parser.add_argument("--max-backoff", type=int, default=64, help="Maximum backoff (seconds) on HTTP 429")
    parser.add_argument("--data-dir", default="./binance_data", help="Directory to store downloaded data")
    args = parser.parse_args()

    start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    main(args.symbol, args.interval, start, end, args.rate_limit_sleep, args.max_backoff, args.data_dir)
