import pandas as pd


def backtest(startDate,endDate):
    jpmDF = pd.read_csv('JPM cleaned.csv')
    wmtDF = pd.read_csv('WMT cleaned.csv')
    avgoDF = pd.read_csv('AVGO cleaned.csv')
    print("loading dataframes")

    curDate = startDate
    """
    date_column = None
    for column in jpmDF.columns:
        if curDate in jpmDF[column].values:
            date_column = column
            break

    if date_column is None:
        raise ValueError(f"Date {curDate} not found in any column of the dataframes.")
    """
    
backtest(5,10)