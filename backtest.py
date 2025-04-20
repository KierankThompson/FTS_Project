import pandas as pd
import datetime
import numpy as np
from arch import arch_model
import time as t
from datetime import time


def backtest(startDate,endDate,startingMoney=1000000,lag=14, logreturns=False, egarchLog=True, pterms=1,oterms=0,qterms=1):
    
    print("loading dataframes...")
    allocations = {'JPM':int(startingMoney/3),'WMT':int(startingMoney/3),'AVGO':int(startingMoney/3)}
    positions = {'JPM':0,"WMT":0,"AVGO":0}
    curMoney = startingMoney

    jpmDF = pd.read_csv('JPM cleaned.csv')
    wmtDF = pd.read_csv('WMT cleaned.csv')
    avgoDF = pd.read_csv('AVGO cleaned.csv')

    jpmDF['day'] = pd.to_datetime(jpmDF['ts_event']).dt.date
    jpmDF['year_month'] = pd.to_datetime(jpmDF['ts_event']).dt.strftime('%Y-%m')
    jpmDF['vwap'] = np.nan 
    jpmDF['volatility'] = np.nan 
    jpmDF.set_index('ts_event', inplace=True)
    jpmDF.index = pd.to_datetime(jpmDF.index)
    jpmDF['min_from_open'] = ((jpmDF.index - jpmDF.index.normalize()) / pd.Timedelta(minutes=1)) - (9 * 60 + 30) + 1
    jpmDF['daily_reutrn'] = np.nan
    jpmDF['daily_log_return'] = np.nan
    jpmDF['dvol'] = np.nan
    jpmDF['egarch_vol'] = np.nan

    wmtDF['day'] = pd.to_datetime(wmtDF['ts_event']).dt.date
    wmtDF['year_month'] = pd.to_datetime(wmtDF['ts_event']).dt.strftime('%Y-%m')
    wmtDF['vwap'] = np.nan 
    wmtDF['volatility'] = np.nan 
    wmtDF.set_index('ts_event', inplace=True)
    wmtDF.index = pd.to_datetime(wmtDF.index)
    wmtDF['min_from_open'] = ((wmtDF.index - wmtDF.index.normalize()) / pd.Timedelta(minutes=1)) - (9 * 60 + 30) + 1
    wmtDF['daily_return'] = np.nan
    wmtDF['daily_log_return'] = np.nan
    wmtDF['dvol'] = np.nan
    wmtDF['egarch_vol'] =  np.nan

    avgoDF['day'] = pd.to_datetime(avgoDF['ts_event']).dt.date
    avgoDF['year_month'] = pd.to_datetime(avgoDF['ts_event']).dt.strftime('%Y-%m')
    avgoDF.set_index('ts_event', inplace=True)
    avgoDF.index = pd.to_datetime(avgoDF.index)
    avgoDF['vwap'] = np.nan
    avgoDF['volatility'] = np.nan 
    avgoDF['min_from_open'] = ((avgoDF.index - avgoDF.index.normalize()) / pd.Timedelta(minutes=1)) - (9 * 60 + 30) + 1
    avgoDF['daily_return'] = np.nan
    avgoDF['daily_log_return'] = np.nan
    avgoDF['dvol'] = np.nan
    avgoDF['egarch_vol'] = np.nan

    dataframes = {'JPM':jpmDF,"WMT":wmtDF,"AVGO":avgoDF}
    
    

    open_time = time(13, 30)
    close_time = time(20, 0)
    print("generating statstics...")
    start = t.time()
    for key, df in dataframes.items():
        daily_groups = df.groupby('day')
        all_days = df['day'].unique()
        for d in range(1, len(all_days)):
            current_day = all_days[d]
            prev_day = all_days[d-1]

            current_day_data = daily_groups.get_group(current_day)
            prev_day_data = daily_groups.get_group(prev_day)
            
            hlc = (current_day_data['high'] + current_day_data['low'] + current_day_data['close']) / 3
            vol_x_hlc = current_day_data['volume'] * hlc
            cum_vol_x_hlc = vol_x_hlc.cumsum()  
            cum_volume = current_day_data['volume'].cumsum()
            df.loc[current_day_data.index, 'vwap'] = cum_vol_x_hlc / cum_volume


            df.loc[current_day_data.index, 'daily_return'] = current_day_data['close'].iloc[-1] / prev_day_data['close'].iloc[-1] - 1
            df.loc[current_day_data.index, 'daily_log_return'] = np.log(current_day_data['close'].iloc[-1] / prev_day_data['close'].iloc[-1])

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
                model = arch_model(training_returns, vol='EGARCH', p=pterms, o=oterms, q=qterms)
                
                res = model.fit(disp='off',show_warning = False, options={'maxiter': 100})
                forecast = res.forecast(horizon=1)
                vol_forecast = np.sqrt(forecast.variance.values[-1, 0]) / 100
                df.loc[current_day_data.index, 'egarch_vol'] = vol_forecast
        monthly_groups = df.groupby('year_month')
        all_months = df['year_month'].unique()
        for m in range(1,len(all_months)):
    
                
                    
            


            
    end = t.time()
    print(f"{end - start}")
                

            
       
    startDT = startDate + ' ' + '13:30:00+00:00'
    

 
    day = '2018-09-24'
    
        
            
        
        
    

    
    
    
    
backtest('2018-09-25','2018-10-25')