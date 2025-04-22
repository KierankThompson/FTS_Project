import pandas as pd


def getBenchmark(startDate,endDate,startingMoney=1000000):
    jpmDF = pd.read_csv('JPM cleaned.csv')
    jpmDF['day'] = pd.to_datetime(jpmDF['ts_event']).dt.date
    jpmDF.set_index('ts_event', inplace=True)
    jpmDF.index = pd.to_datetime(jpmDF.index)
    wmtDF = pd.read_csv('WMT cleaned.csv')
    wmtDF['day'] = pd.to_datetime(wmtDF['ts_event']).dt.date
    wmtDF.set_index('ts_event', inplace=True)
    wmtDF.index = pd.to_datetime(wmtDF.index)
    vDF = pd.read_csv('V cleaned.csv')
    vDF['day'] = pd.to_datetime(vDF['ts_event']).dt.date
    vDF.set_index('ts_event', inplace=True)
    vDF.index = pd.to_datetime(vDF.index)
    dataframes = {'JPM': jpmDF, "WMT": wmtDF, "V": vDF}
    money_tracker = {key: startingMoney * 0.33 for key in dataframes.keys()}

    daily_groups = {key: df.groupby('day') for key, df in dataframes.items()}
    all_days = sorted(set(day for df in dataframes.values() for day in df['day'].unique()))
    start_date_index = all_days.index(pd.to_datetime(startDate).date())
    end_date_index = all_days.index(pd.to_datetime(endDate).date()) if pd.to_datetime(endDate).date() in all_days else len(all_days)
    stats = pd.DataFrame()
    stats["JPM Assets"] = 0
    stats["JPM Return"] = 0
    stats["WMT Assets"] = 0
    stats["WMT Return"]
    stats["V Assets"] = 0
    stats["V Return"]
    stats["Total Assets"] = 0

    for d in range(start_date_index, end_date_index):
        current_day = all_days[d]
        prev_day = all_days[d-1]
        for key, df in dataframes.items():
            if current_day in daily_groups[key].groups and prev_day in daily_groups[key].groups:
                prev_close = daily_groups[key].get_group(prev_day)['price_close'].iloc[-1]
                current_close = daily_groups[key].get_group(current_day)['price_close'].iloc[-1]
                percent_return = (current_close - prev_close) / prev_close
                stats.loc[current_day, f'{key} Return'] = percent_return
                money_tracker[key] *= (1 + percent_return)
                stats.loc[current_day, f'{key} Assets'] = money_tracker[key]
        stats.loc[current_day, 'Total Assets'] = stats.loc[current_day, 'JPM Assets'] + stats.loc[current_day, 'WMT Assets'] + stats.loc[current_day, 'V Assets']
    
    print(f"results written to benchmark{startDate}-{endDate}.csv")
    stats.to_csv(f'benchmark{startDate}-{endDate}.csv', index=True)

            









