from pybit.unified_trading import HTTP
import pandas as pd

#*GET last four hour candle kline(1000 candles)
def get_four_hour_kline(session, end_time, symbol, add):
    #get long cline
    kline = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=240,
        end=end_time,
        limit=1000
    )["result"]["list"]
    addition = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=60,
        end=end_time-1000*4*60*60*1000,
        limit=add
    )["result"]["list"]
    kline+=addition
    #getting candle list
    candle_list=kline
    
    return candle_list

#*GET last one hour candle kline(1000 candles)
def get_one_hour_kline(session, end_time, symbol, add):
    kline = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=60,
        end=end_time,
        limit=1000
    )["result"]["list"]
    addition = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=60,
        end=end_time-1000*60*60*1000,
        limit=add
    )["result"]["list"]
    kline+=addition
    #getting candle list
    candle_list=kline
    
    return candle_list

#*GET last one minute candle kline(1000 candles)
def get_one_minute_kline(session, end_time, symbol, add1000, add):
    candle_list=[]
    for i in range(add1000+1):
        kline = session.get_kline(
            category="linear",
            symbol=symbol,
            interval=1,
            end=end_time-i*60000000, 
            limit=1000
        )["result"]["list"]
        candle_list+=kline
    addition = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=1,
        end=end_time-(add1000+1)*60000000,
        limit=add
    )["result"]["list"]
    candle_list+=addition

    #getting candle list
    return candle_list

#*Getting real values for last candle
def to_real_values(candle_list_1h, candle_list_4h, candle_list_1m):
    #get index of first 1m candle of this 1h candle (https://ru.stackoverflow.com/questions/1292640/Как-получить-элементы-из-вложенного-списка)
    candle_list_1m_1h_end=[float(x[0]) for x in candle_list_1m[0:60]].index(float(candle_list_1h[0][0]))
    #get all 1m candles, that was created from start of this 1H candle
    candle_list_1m_1h=candle_list_1m[0:candle_list_1m_1h_end+1]
    #get index of first 1m candle of this 4h candle (https://ru.stackoverflow.com/questions/1292640/Как-получить-элементы-из-вложенного-списка)
    candle_list_1m_4h_end=[float(x[0]) for x in candle_list_1m[0:240]].index(float(candle_list_4h[0][0]))
    #get all 1m candles, that was created from start of this 4H candle
    candle_list_1m_4h=candle_list_1m[0:candle_list_1m_4h_end+1]

    #getting last 1H candle top and min prices
    tops=[]
    mins=[]
    vol=[]
    turnover=[]
    for candle in candle_list_1m_1h:
        #convert to float
        for mark in range(len(candle)):
            candle[mark]=float(candle[mark])
        tops.append(candle[2])
        mins.append(candle[3])
        vol.append(candle[5])
        turnover.append(candle[6])
        
    #settin last candle price to actually(minute) price
    candle_list_1h[0][4]=candle_list_1m[0][4]
    candle_list_1h[0][3]=min(mins)
    candle_list_1h[0][2]=max(tops)
    candle_list_1h[0][5]=sum(vol)
    candle_list_1h[0][6]=sum(turnover)
    
    tops=[]
    mins=[]
    vol=[]
    turnover=[]
    for candle in candle_list_1m_4h:
        #convert to float
        for mark in range(len(candle)):
            candle[mark]=float(candle[mark])
        tops.append(candle[2])
        mins.append(candle[3])
        vol.append(candle[5])
        turnover.append(candle[6])

    #settin last 4H candle price to actually(minute) price
    candle_list_4h[0][4]=candle_list_1m[0][4]
    candle_list_4h[0][3]=min(mins)
    candle_list_4h[0][2]=max(tops)
    candle_list_4h[0][5]=sum(vol)
    candle_list_4h[0][6]=sum(turnover)
    
    return [candle_list_1h, candle_list_4h]

def get_fund_rate(session, end_time, symbol, add):
    funding_rate = session.get_funding_rate_history(
        category="linear",
        symbol=symbol,
        limit=200,
        endTime=end_time)["result"]["list"]
    deal_fund_rate=session.get_funding_rate_history(
        category="linear",
        symbol=symbol,
        limit=add,
        endTime=end_time-200*60*8*60*1000)["result"]["list"]
    funding_rate+=deal_fund_rate
    
    return funding_rate