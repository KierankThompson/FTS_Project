import pandas as pd


def backtest(startDate,endDate,startingMoney=1000000):
    allocations = {'JPM':0.33,'WMT':0.33,'AVGO':0.33}
    positions = {'JPM':0,"WMT":0,"AVGO":0}
    curMoney = startingMoney
    jpmDF = pd.read_csv('JPM cleaned.csv')
    wmtDF = pd.read_csv('WMT cleaned.csv')
    avgoDF = pd.read_csv('AVGO cleaned.csv')
    print("loading dataframes...")
    startDT = startDate + ' ' + '13:30:00+00:00'

    jpmStart = jpmDF.index[jpmDF['ts_event'] == startDT][0]
    wmtStart = wmtDF.index[wmtDF['ts_event'] == startDT][0]
    avgoStart = avgoDF.index[avgoDF['ts_event'] == startDT][0]
    if jpmStart != wmtStart != avgoStart:
        raise Exception("CSV's out of order")
    i = jpmStart
    day = '2018-09-24'
    while jpmDF.loc[i,'ts_event'].split()[0] != endDate:
        #jpm first
        jpmDate, jpmTime = jpmDF.loc[i,'ts_event']
        jpmOpen = jpmDF.loc[i,'open']
        jpmHigh = jpmDF.loc[i,'high']
        jpmLow = jpmDF.loc[i,'low']
        jpmClose = jpmDF.loc[i,'close']
        jpmVol = jpmDF.loc[i,'volume']

        wmtDate, wmtTime = wmtDF.loc[i, 'ts_event']
        wmtOpen = wmtDF.loc[i, 'open']
        wmtHigh = wmtDF.loc[i, 'high']
        wmtLow = wmtDF.loc[i, 'low']
        wmtClose = wmtDF.loc[i, 'close']
        wmtVol = wmtDF.loc[i, 'volume']

        avgoDate, avgoTime = avgoDF.loc[i, 'ts_event']
        avgoOpen = avgoDF.loc[i, 'open']
        avgoHigh = avgoDF.loc[i, 'high']
        avgoLow = avgoDF.loc[i, 'low']
        avgoClose = avgoDF.loc[i, 'close']
        avgoVol = avgoDF.loc[i, 'volume']

        if jpmDate != day:
            jpmDayOpen = jpmOpen
            wmtDayOpen = avgoOpen
            avgoDayOpen = jpmOpen
            jpmDayClose = jpmDF.loc[i-1,'close']
            wmtDayClose = wmtDF.loc[i-1,'close']
            avgoDayClose = avgoDF.loc[i-1,'close']
           
            
        
        
    

    
    
    
    
backtest('2018-09-25',10)