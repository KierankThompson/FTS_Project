import pandas as pd
from datetime import datetime, timedelta

# Config
csv_path = "visa.csv"  # Replace this
timestamp_col = "ts_event"

# Load and parse timestamps
df = pd.read_csv(csv_path)
df[timestamp_col] = pd.to_datetime(df[timestamp_col], utc=True)
df.set_index(timestamp_col, inplace=True)
df = df.sort_index()

# Create a container for processed data
filled_days = []

# Group by date
for date, group in df.groupby(df.index.date):
    # Define start and end times for the trading day
    day_start = datetime.combine(date, datetime.min.time()).replace(tzinfo=pd.Timestamp.utcnow().tz)
    start_dt = day_start + timedelta(hours=13, minutes=30)
    end_dt = day_start + timedelta(hours=20)

    # Only keep data up to that day's end
    group = group[group.index <= end_dt]

    if group.empty or group.index.max() < start_dt:
        continue  # Skip this day if no data in range

    # Create full datetime range for each minute in trading day
    full_range = pd.date_range(start=start_dt, end=end_dt, freq='T', tz="UTC")

    # Reindex to full range and fill missing with last available value
    group = group.reindex(full_range, method='ffill')

    # Keep only from the first available data point
    first_valid = group.first_valid_index()
    group = group[group.index >= first_valid]

    filled_days.append(group)

# Combine all processed days
final_df = pd.concat(filled_days)
final_df.reset_index(inplace=True)
final_df.rename(columns={'index': timestamp_col}, inplace=True)

# Save output
final_df.to_csv("filled_output.csv", index=False)
print("Filled data saved to filled_output.csv")
