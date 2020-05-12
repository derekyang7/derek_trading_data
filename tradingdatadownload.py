import numpy
import pandas as pd
import json
#https://pypi.org/project/yahooquery
from yahooquery import Ticker
import datetime
import pyodbc
from sqlalchemy import create_engine
import sys, getopt

def saveToTable(df, tableName, cnxn, engine):
    
    # remove all existing data from database table
    cursor = cnxn.cursor()
    cursor.execute('delete ' + tableName)
    cursor.commit()
    cursor.close()
    
    df.to_sql(tableName, engine, if_exists='append',schema='dbo', index=True)

def downloadDaily(ticker_symbols, cnxn, engine):
    
    #cursor = cnxn.cursor()
    #cursor.execute('Delete Daily')
    #cursor.commit()
    #cursor.close()

    for symbol in ticker_symbols:
        ticker = Ticker(symbol)
        #daily = ticker.history(period='1y', interval='1d')
        #print('Download daily completed')

        #daily.to_sql('Daily', engine, if_exists='append',schema='dbo', index=True)

        #print('Saved to table daily')

        df_options = ticker.option_chain
        df_options['createDate'] = datetime.date.today()

        df_options.to_sql('Option', engine, if_exists='append',schema='dbo', index=True)


def main(argv):
    interval = ''
    
    try:
        opts, args = getopt.getopt(argv,"hi:",["interval="])
    except getopt.GetoptError:
        print('tradingdatadownload.py -i <interval>')
        sys.exit(2)
    
    for opt, arg in opts:
        if opt == '-h':
            print('tradingdatadownload.py -i <interval>')
            sys.exit()
        elif opt in ("-i", "--interval"):
            interval = arg.lstrip()

    if interval == '':
        print('tradingdatadownload.py -i <interval>')
        sys.exit(2)


    #cnxn = pyodbc.connect("dsn=azure-trading;UID=sqladmin;PWD=Duoduo88")
    #engine = create_engine("mssql+pyodbc://sqladmin:Duoduo88@azure-trading")
    cnxn = pyodbc.connect("DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.1.19;UID=sa;PWD=Duoduo88;database=trading")
    engine = create_engine("mssql+pyodbc://sa:Duoduo88@192.168.1.19/trading?driver=ODBC+Driver+17+for+SQL+Server")
    sym = pd.read_sql_table("Symbol", engine)
    ticker_symbols = sym['Symbol'].values.tolist()
    print(ticker_symbols)
    tickers = Ticker(ticker_symbols)

    if interval == 'weekly':   
        weekly = tickers.history(period='5y', interval='1wk')
        print('Download weekly completed')
        saveToTable(weekly, 'Weekly', cnxn, engine)
        print('Saved to table weekly')

    if interval == 'daily':
        daily = tickers.history('1mo', interval='1d')
        saveToTable(daily, 'Daily', cnxn, engine)
        downloadDaily(ticker_symbols, cnxn, engine)

    if interval == 'hourly':
        hourly = tickers.history(period='1mo', interval='1h')
        print('Download hourly completed')
        saveToTable(hourly, 'Hourly', cnxn, engine)
        print('Saved to table hourly')



if __name__ == '__main__':
    main(sys.argv[1:])