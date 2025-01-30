from collect_data_lib_V5 import get_four_hour_kline, get_one_minute_kline, get_one_hour_kline, get_fund_rate, to_real_values
from create_datasets_V5 import create_datasets
from pybit.unified_trading import HTTP
import requests
import pandas as pd
import datetime
import time
import os
import math


#!FUNCTION TO GET ALL INFO
def cycle_get_info(symbol, full_1h_candle_list, full_4h_candle_list, one_minute_candle_list, full_funding_rate):
    
    global h1_skips
    global h4_skips
    global fund_rate_skips
    
    if len(full_1h_candle_list)>1000+h1_skips:
        #get this time gap 1Hcl and 4Hcl
        if int(one_minute_candle_list[0][0])>=int(full_1h_candle_list[-1001-h1_skips][0]):
            print("skipped 1H")
            h1_skips+=1
    this_1h_candle_list=full_1h_candle_list[-(1000+h1_skips):len(full_1h_candle_list)-h1_skips]
    if len(full_4h_candle_list)>1000+h4_skips:
        if int(one_minute_candle_list[0][0])>=int(full_4h_candle_list[-1001-h4_skips][0]):
            print("skipped 4H")
            h4_skips+=1
    this_4h_candle_list=full_4h_candle_list[-(1000+h4_skips):len(full_4h_candle_list)-h4_skips]
    
    #get real values for 1Hcl and 4Hcl
    rv_response=to_real_values(this_1h_candle_list, this_4h_candle_list, one_minute_candle_list)
    this_1h_candle_list=rv_response[0]
    this_4h_candle_list=rv_response[1]
    #get fund rate
    if len(full_funding_rate)>200+fund_rate_skips:
        if int(one_minute_candle_list[0][0])>=int(full_funding_rate[-201-fund_rate_skips]["fundingRateTimestamp"]):
            print("skipped FR")
            fund_rate_skips+=1
    fund_rate=full_funding_rate[-(200+fund_rate_skips):len(full_funding_rate)-fund_rate_skips]
    
    return [last_deal, deal_now, symbol, this_4h_candle_list, this_1h_candle_list, one_minute_candle_list, fund_rate]


#!MAIN FUNCTION
#settting basic values
all_datafiles=[[],[],[],[],[],[],[]]
h1_skips=0
h4_skips=0
fund_rate_skips=0

#getting last deal
last_deal = input("Enter last deal: ")
deal_now = input("Enter deal: ")
#getting mode
mode=int(input("Enter mode 1-contin/0-always restart"))

#get symbol and year. I SET DEFAULT PARAMS!!!!!!!
#symbol=input("enter symbol: ")
symbol="BTCUSDT"
year=2023

#getting deal start time
date_start=datetime.datetime(year,
                            int(input("Enter start month: ")),
                            int(input("Enter start day: ")),
                            int(input("Enter start hour: ")),
                            int(input("Enter start minute: ")))

#getting deal end time
date_end=datetime.datetime(year,
                            int(input("Enter end month: ")),
                            int(input("Enter end day: ")),
                            int(input("Enter end hour: ")),
                            int(input("Enter end minute: ")))


FULL_START_TIME=time.time()

#getting time
bybit_timenow_response = eval(requests.get("https://api.bybit.com/v5/market/time").text)

#getting time gap of deal
minutes_in_moment=int((date_end-date_start).total_seconds() // 60)
time_from_now=round((datetime.datetime.now()-date_start).total_seconds())

#starting bybit http session to get klines
session = HTTP()


time_END=(int(bybit_timenow_response["result"]["timeSecond"]) - int(time_from_now))*1000+minutes_in_moment*60000
full_4h_candle_list=get_four_hour_kline(session, time_END, symbol, math.ceil(minutes_in_moment/240))
full_1h_candle_list=get_one_hour_kline(session, time_END, symbol, math.ceil(minutes_in_moment/60))
full_1m_candle_list=get_one_minute_kline(session, time_END, symbol, minutes_in_moment//1000, minutes_in_moment%1000)
full_funding_rate=get_fund_rate(session, time_END, symbol, minutes_in_moment//480)
print(f"Main info downloaded {len(full_4h_candle_list)}, {len(full_1h_candle_list)}, {len(full_1m_candle_list)}")

#!REACTOR
#get every candle in deal(one minute gap)
for i in range(minutes_in_moment):  
     
    this_1m_candle_list=full_1m_candle_list[-1000-i:minutes_in_moment+1000-i]
    
    time_END=(int(bybit_timenow_response["result"]["timeSecond"]) - int(time_from_now))*1000 + i*60000

    ans = cycle_get_info(symbol, full_1h_candle_list, full_4h_candle_list, this_1m_candle_list, full_funding_rate)      

    #here we convert data in pd datasets
    datas=create_datasets(ans[0], ans[1], ans[2], ans[3], ans[4], ans[5], ans[6])
    
    #distributing all datafiles to groups: ND, FR, 4H, 1H... 
    for data_num in range(7):
        all_datafiles[data_num].append(datas[data_num])
    
    #setting mode for next cycle    
    if mode==1:
        last_deal = deal_now
    
    print(f"Loading({i}/{minutes_in_moment}) - {(i+1)/minutes_in_moment*100}%")
print("Installing")


#!CONCANTINATING AND SAVING CSVS
final_pd_frames=[]
for data_class in all_datafiles:
    data_class=pd.concat(data_class)
    final_pd_frames.append(data_class)
    
folders=os.listdir(path="datafiles")
for dataset_n in range(len(final_pd_frames)):
    df = final_pd_frames[dataset_n]
    df.reset_index()
    file_number=len(os.listdir(path=f"datafiles\\{folders[dataset_n]}"))
    df.to_csv(f"datafiles\\{folders[dataset_n]}\\data{file_number}.csv")

print("Ready")
FULL_END_TIME=time.time()
print(FULL_END_TIME-FULL_START_TIME)