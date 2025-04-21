import pandas as pd
import numpy as np
from arch import arch_model
from sklearn.linear_model import LinearRegression
from datetime import datetime


def backtest(startDate,endDate,startingMoney=1000000, lag=14, jpm_vol= 0.01, wmt_vol=0.01, v_vol=0.01, targetvol = 0.2, logreturns=False, egarchLog=True, pterms=1, oterms=0, qterms=1, tradefreq=30, maxLeverage=2):
    
    print("loading dataframes...")
    allocations = {'JPM':0.33,'WMT':0.33,'V':0.33}
    cost_basis = {'JPM':0,"WMT":0,"V":0}
    numShares = {'JPM':0,"WMT":0,"V":0}
    targetVol = {'JPM':jpm_vol,"WMT":wmt_vol,"V":v_vol}
    curMoney = startingMoney

    jpmDF = pd.read_csv('JPM cleaned.csv')
    wmtDF = pd.read_csv('WMT cleaned.csv')
    vDF = pd.read_csv('V cleaned.csv')

    jpmDF['day'] = pd.to_datetime(jpmDF['ts_event']).dt.date
    jpmDF['year_month'] = pd.to_datetime(jpmDF['ts_event']).dt.strftime('%Y-%m')
    jpmDF['vwap'] = np.nan 
    jpmDF['volatility'] = np.nan 
    jpmDF.set_index('ts_event', inplace=True)
    jpmDF.index = pd.to_datetime(jpmDF.index)
    jpmDF['min_from_open'] = ((jpmDF.index - jpmDF.index.normalize()) / pd.Timedelta(minutes=1)) - (13 * 60 + 30) + 1
    jpmDF['daily_return'] = np.nan
    jpmDF['daily_log_return'] = np.nan
    jpmDF['dvol'] = np.nan
    jpmDF['egarch_vol'] = np.nan
    jpmDF['open_return'] = np.nan
    jpmDF['intercept'] = np.nan
    jpmDF['coef'] = np.nan
    jpmDF['prev_month_vol'] = np.nan

    wmtDF['day'] = pd.to_datetime(wmtDF['ts_event']).dt.date
    wmtDF['year_month'] = pd.to_datetime(wmtDF['ts_event']).dt.strftime('%Y-%m')
    wmtDF['vwap'] = np.nan 
    wmtDF['volatility'] = np.nan 
    wmtDF.set_index('ts_event', inplace=True)
    wmtDF.index = pd.to_datetime(wmtDF.index)
    wmtDF['min_from_open'] = ((wmtDF.index - wmtDF.index.normalize()) / pd.Timedelta(minutes=1)) - (13 * 60 + 30) + 1
    wmtDF['daily_return'] = np.nan
    wmtDF['daily_log_return'] = np.nan
    wmtDF['dvol'] = np.nan
    wmtDF['egarch_vol'] =  np.nan
    wmtDF['open_return'] = np.nan
    wmtDF['intercept'] = np.nan
    wmtDF['coef'] = np.nan
    wmtDF['prev_month_vol'] = np.nan

    vDF['day'] = pd.to_datetime(vDF['ts_event']).dt.date
    vDF['year_month'] = pd.to_datetime(vDF['ts_event']).dt.strftime('%Y-%m')
    vDF.set_index('ts_event', inplace=True)
    vDF.index = pd.to_datetime(vDF.index)
    vDF['vwap'] = np.nan
    vDF['volatility'] = np.nan 
    vDF['min_from_open'] = ((vDF.index - vDF.index.normalize()) / pd.Timedelta(minutes=1)) - (13 * 60 + 30) + 1
    vDF['daily_return'] = np.nan
    vDF['daily_log_return'] = np.nan
    vDF['dvol'] = np.nan
    vDF['egarch_vol'] = np.nan
    vDF['open_return'] = np.nan
    vDF['intercept'] = np.nan
    vDF['coef'] = np.nan
    vDF['prev_month_vol'] = np.nan

    dataframes = {'JPM':jpmDF,"WMT":wmtDF,"V":vDF}

    
    
    

    
    print("generating statstics...")
    
    for key, df in dataframes.items():
        daily_groups = df.groupby('day')
        all_days = df['day'].unique()
        for d in range(1, len(all_days)):
            current_day = all_days[d]
            prev_day = all_days[d-1]

            if current_day > pd.to_datetime(endDate).date():
                break

            current_day_data = daily_groups.get_group(current_day)
            prev_day_data = daily_groups.get_group(prev_day)
            
            hlc = (current_day_data['high'] + current_day_data['low'] + current_day_data['close']) / 3
            vol_x_hlc = current_day_data['volume'] * hlc
            cum_vol_x_hlc = vol_x_hlc.cumsum()  
            cum_volume = current_day_data['volume'].cumsum()
            df.loc[current_day_data.index, 'vwap'] = cum_vol_x_hlc / cum_volume


            df.loc[current_day_data.index, 'daily_return'] = current_day_data['close'].iloc[-1] / prev_day_data['close'].iloc[-1] - 1
            df.loc[current_day_data.index, 'daily_log_return'] = np.log(current_day_data['close'].iloc[-1] / prev_day_data['close'].iloc[-1])

            
            try:
                df.loc[current_day_data.index, 'open_return'] = current_day_data.loc[current_day_data['min_from_open'] == 30.0, 'close'].iloc[0] / prev_day_data['close'].iloc[-1] - 1
            except Exception:
                df.loc[current_day_data.index, 'open_return'] = current_day_data.loc[current_day_data['min_from_open'] == 90.0, 'close'].iloc[0] / prev_day_data['close'].iloc[-1] - 1
            
                

            if d > lag:
                recent_days = all_days[d - lag:d]
                
                if not logreturns:
                    daily_vals = df[df['day'].isin(recent_days)].groupby('day')['daily_return'].last()
                    df.loc[current_day_data.index, 'dvol'] = daily_vals.std()
                daily_vals = df[df['day'].isin(recent_days)].groupby('day')['daily_log_return'].last()

                if logreturns:
                    df.loc[current_day_data.index, 'dvol'] = daily_vals.std()

                if not egarchLog:
                    daily_vals = df[df['day'].isin(recent_days)].groupby('day')['daily_return'].last()
                

                training_returns = daily_vals.values * 100
                model = arch_model(training_returns, vol='GARCH', p=pterms, o=oterms, q=qterms)
                res = model.fit(disp='off', show_warning = False, options={'maxiter': 100})
                forecast = res.forecast(horizon=1)
                vol_forecast = np.sqrt(forecast.variance.values[-1, 0]) / 100
                df.loc[current_day_data.index, 'egarch_vol'] = vol_forecast
        monthly_groups = df.groupby('year_month')
        all_months = df['year_month'].unique()
        
        
        for m in range(1, len(all_months)):
            if pd.to_datetime(all_months[m] + '-01').date() > (pd.to_datetime(endDate) + pd.offsets.MonthBegin(1)).date():
                break

            current_month = all_months[m]
            prev_month = all_months[m - 1]
            


            current_month_data = monthly_groups.get_group(current_month)
            prev_month_data = monthly_groups.get_group(prev_month)


            
        
            prev_month_days = prev_month_data['day'].unique()
            prev_month_data = prev_month_data[prev_month_data['day'].isin(prev_month_days)]

            std_daily_return = prev_month_data.groupby('day')['daily_return'].first().std()
            
            df.loc[current_month_data.index, 'prev_month_vol'] = std_daily_return
            X = prev_month_data.groupby('day')['open_return'].first().dropna().values.reshape(-1, 1)
            y = prev_month_data.groupby('day')['daily_return'].first().dropna().values
            model = LinearRegression()
            
            
            model.fit(X, y)
          
            df.loc[current_month_data.index, 'intercept'] = model.intercept_
            df.loc[current_month_data.index, 'coef'] = model.coef_[0]




    daily_groups = {key: df.groupby('day') for key, df in dataframes.items()}
    all_days = sorted(set(day for df in dataframes.values() for day in df['day'].unique()))
    end_date_index = all_days.index(pd.to_datetime(endDate).date()) if pd.to_datetime(endDate).date() in all_days else len(all_days)

    stats = pd.DataFrame()

    stats['aum'] = startingMoney
    

    
    stats['JPM Allocation'] = np.nan
    stats['JPM Buy'] = np.nan
    stats['JPM Sell'] = np.nan
    stats['JPM Shares'] = np.nan
    stats['JPM Cost Basis'] = np.nan
    stats['JPM Upper'] = np.nan
    stats['JPM Lower'] = np.nan
    stats['JPM Average VWAP'] = np.nan
    stats['WMT Ret'] = np.nan
 
    
    
    stats['WMT Allocation'] = np.nan
    stats['WMT Buy'] = np.nan
    stats['WMT Sell'] = np.nan
    stats['WMT Shares'] = 0
    stats['WMT Cost Basis'] = 0
    stats['WMT Upper'] = np.nan
    stats['WMT Lower'] = np.nan
    stats['WMT Average VWAP'] = np.nan
    stats['WMT Ret'] = np.nan

    
    stats['V Allocation'] = np.nan
    stats['V Buy'] = np.nan
    stats['V Sell'] = np.nan
    stats['V Shares'] = 0
    stats['V Cost Basis'] = 0
    stats['V Upper'] = np.nan
    stats['V Lower'] = np.nan
    stats['V Average VWAP'] = np.nan
    stats['V Ret'] = np.nan

    monthSeen = set([pd.to_datetime(startDate).strftime('%Y-%m')])
    print("starting backtest...")
    totalpnl = 0
    for d in range(all_days.index(pd.to_datetime(startDate).date()), end_date_index):
        current_day = all_days[d]
        prev_day = all_days[d-1]
        
        current_month = pd.to_datetime(current_day).strftime('%Y-%m')
        if current_month not in monthSeen:
            monthSeen.add(current_month)
            jpmVol = targetvol / jpmDF.loc[jpmDF['day'] == current_day, 'prev_month_vol'].iloc[0] * (1/3)
            wmtVol = targetvol / wmtDF.loc[wmtDF['day'] == current_day, 'prev_month_vol'].iloc[0] * (1/3)
            vVol = targetvol /vDF.loc[vDF['day'] == current_day, 'prev_month_vol'].iloc[0] * (1/3)
            totalVol = jpmVol + wmtVol + vVol
            allocations['JPM'] = round(jpmVol / totalVol,2)
            allocations['WMT'] = round(wmtVol / totalVol,2)
            allocations['V'] = round(vVol / totalVol,2)
        if current_day not in stats.index:
            if prev_day in stats.index:
                stats.loc[current_day, 'aum'] = stats.loc[prev_day, 'aum']
            else:
                stats.loc[current_day, 'aum'] = startingMoney
        
        for key, df in dataframes.items():
           
            if current_day > pd.to_datetime(endDate).date():
                break

            if current_day not in daily_groups[key].groups or pd.isna(df.loc[daily_groups[key].get_group(current_day).index, 'intercept']).all():
                continue

            current_day_data = daily_groups[key].get_group(current_day)
            prev_day_data = daily_groups[key].get_group(prev_day)

            stats.loc[current_day, f'{key} Allocation'] = allocations[key]
            previous_aum = 0
            prev_basis = 0
            prev_shares = 0
            if prev_day in stats.index:
                prev_shares = stats.loc[prev_day, f'{key} Shares']
                prev_basis = stats.loc[prev_day, f'{key} Cost Basis']
                previous_aum = stats.loc[prev_day, 'aum']
            else:
                prev_shares = 0
                prev_basis = 0
                previous_aum = startingMoney
            
            open_price = current_day_data['open'].iloc[0]
            prev_close_price = prev_day_data['close'].iloc[-1]
            upper_bound = max(open_price, prev_close_price) * (1+current_day_data['egarch_vol'].iloc[0])
            lower_bound = min(open_price, prev_close_price) * (1-current_day_data['egarch_vol'].iloc[0])
            stats.loc[current_day, f'{key} Upper'] = upper_bound
            stats.loc[current_day, f'{key} Lower'] = upper_bound


            predicted_close_return = current_day_data['intercept'].iloc[0] + current_day_data['coef'].iloc[0]*current_day_data['open_return'].iloc[0]
           
            regressionSignal = False
            if (current_day_data['open_return'].iloc[0] * predicted_close_return) > 0:
                regressionSignal = True
            

            close_prices = current_day_data['close']
            vwap = current_day_data['vwap']
            stats.loc[current_day, f'{key} Average VWAP'] = current_day_data['vwap'].mean()
            signals = np.zeros_like(close_prices)
            signals[(close_prices > upper_bound) & (close_prices > vwap)] = 1
            signals[(close_prices < lower_bound) & (close_prices < vwap)] = -1
            
            trade_indices = np.where((current_day_data["min_from_open"] % tradefreq == 0) & (current_day_data["min_from_open"] > 30))[0]
            exposure = np.full(len(current_day_data), np.nan)  
            exposure[trade_indices] = signals[trade_indices]  
            
            last_valid = np.nan  
            filled_values = []  
            for value in exposure:
                if not np.isnan(value):  
                    last_valid = value
                if last_valid == 0:  
                    last_valid = np.nan
                filled_values.append(last_valid)

            curPos = 0
            exposure = pd.Series(filled_values, index=current_day_data.index).shift(1).fillna(0).values
            gross_pnl = 0
            share_price = 0
            num_shares = 0
            purchase_cost = 0
            for i in range(len(exposure)):
                    if exposure[i] > 0 and curPos == 0 and regressionSignal:
                        if current_day_data['open_return'].iloc[0] > 0 and predicted_close_return > 0:
                            share_price = current_day_data['close'].iloc[i]
                            num_shares = round(round(previous_aum * allocations[key]) / share_price * min(targetVol[key] / current_day_data['dvol'].iloc[0], maxLeverage))
                            curPos = num_shares
                            stats.loc[current_day, f'{key} Buy'] = current_day_data.index[i].time().strftime('%H:%M:%S')
                    elif exposure[i] < 0 and curPos == 0 and regressionSignal:
                        if current_day_data['open_return'].iloc[0] < 0 and predicted_close_return < 0:
                            share_price = current_day_data['close'].iloc[i]
                            num_shares = round(round(previous_aum * allocations[key]) / share_price * min(targetVol[key] / current_day_data['dvol'].iloc[0], maxLeverage))
                            stats.loc[current_day, f'{key} Sold'] = current_day_data.index[i].time().strftime('%H:%M:%S')
                            if prev_shares > 0:
                                num_shares_to_sell = min(prev_shares, num_shares)
                                gross_pnl += num_shares_to_sell * (prev_basis- share_price)
                                prev_shares -= num_shares_to_sell
                                num_shares = max(num_shares - num_shares_to_sell, 0)
                            curPos = -num_shares
                    elif exposure[i] == 0:
                        if curPos < 0:
                            stats.loc[current_day, f'{key} Buy'] = current_day_data.index[i].time().strftime('%H:%M:%S')
                            gross_pnl += (share_price - current_day_data['close'].iloc[i]) * num_shares
                            curPos = 0
                            share_price = 0
                            num_shares = 0
                            prev_basis = 0
                            break
                        elif curPos > 0:
                            stats.loc[current_day, f'{key} Sold'] = current_day_data.index[i].time().strftime('%H:%M:%S')
                            gross_pnl += (current_day_data['close'].iloc[i] - share_price) * num_shares
                            gross_pnl += (current_day_data['close'].iloc[i] - prev_basis) * prev_shares
                            curPos = 0
                            share_price = 0
                            prev_basis = 0
                            prev_shares = 0
                            break
            if curPos < 0:
                gross_pnl = (share_price - current_day_data['close'].iloc[-1]) * num_shares
                curPos = 0
                share_price = 0
            
            if (prev_shares + curPos) != 0:
                prev_basis = ((prev_shares * prev_basis) + (curPos * share_price)) / (prev_shares + curPos)
            else:
                prev_basis = 0
            prev_shares += curPos
            stats.loc[current_day, f'{key} Cost Basis'] = prev_basis
            stats.loc[current_day, f'{key} Shares'] = prev_shares
        
            totalpnl += gross_pnl
            stats.loc[current_day, f'{key} ret'] = gross_pnl / previous_aum
            stats.loc[current_day, 'aum'] += gross_pnl 


    print("-------------------")
    print("")
    print(f"Total Profit: {totalpnl}")
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    print(f"results written to backtest{current_time}.csv")
    stats.to_csv(f'backtest{current_time}.csv', index=True)

 
    

        
            
        
