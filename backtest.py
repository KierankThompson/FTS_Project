import pandas as pd
import datetime


def backtest(startDate,endDate,startingMoney=1000000,vwapLag=14):
    print("loading dataframes...")
    allocations = {'JPM':int(startingMoney/3),'WMT':int(startingMoney/3),'AVGO':int(startingMoney/3)}
    positions = {'JPM':0,"WMT":0,"AVGO":0}
    curMoney = startingMoney
    jpmDF = pd.read_csv('JPM cleaned.csv')
    wmtDF = pd.read_csv('WMT cleaned.csv')
    avgoDF = pd.read_csv('AVGO cleaned.csv')
    jpmDF['day'] = pd.to_datetime(jpmDF['ts_event']).dt.date
    jpmDF['year_month'] = pd.to_datetime(jpmDF['ts_event']).dt.strftime('%Y-%m')
    jpmDF.set_index('ts_event', inplace=True)
    wmtDF['day'] = pd.to_datetime(wmtDF['ts_event']).dt.date
    wmtDF['year_month'] = pd.to_datetime(wmtDF['ts_event']).dt.strftime('%Y-%m')
    wmtDF.set_index('ts_event', inplace=True)
    avgoDF['day'] = pd.to_datetime(avgoDF['ts_event']).dt.date
    avgoDF['year_month'] = pd.to_datetime(avgoDF['ts_event']).dt.strftime('%Y-%m')
    avgoDF.set_index('ts_event', inplace=True)
    dataframes = {'JPM':jpmDF,"WMT":wmtDF,"AVGO":avgoDF}

    all_days = jpmDF['day'].unique()
    all_months = jpmDF['year_month'].unique()
       
    startDT = startDate + ' ' + '13:30:00+00:00'
    
    jpmStart = jpmDF.index[jpmDF['ts_event'] == startDT][0]
    wmtStart = wmtDF.index[wmtDF['ts_event'] == startDT][0]
    avgoStart = avgoDF.index[avgoDF['ts_event'] == startDT][0]
    if jpmStart != wmtStart != avgoStart:
        raise Exception("CSV's out of order")
    i = jpmStart
    day = '2018-09-24'
    
    while jpmDF.loc[i,'ts_event'].split()[0] <= endDate:
        
            
        
        
    

    
    
    
    
backtest('2018-09-25','2018-09-29')