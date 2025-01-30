import pandas as pd

def create_datasets(last_deal, what_to_do_now, symbol, candle_list_4h, candle_list_1h, candle_list_1m, fund_rate):
    dataset_4h={}
    for i in range(1000):
        dataset_4h[f"4H_startTime{i}"] = candle_list_4h[i][0]
        dataset_4h[f"4H_openPrice{i}"] = candle_list_4h[i][1]
        dataset_4h[f"4H_highPrice{i}"] = candle_list_4h[i][2]
        dataset_4h[f"4H_lowPrice{i}"] = candle_list_4h[i][3]
        dataset_4h[f"4H_closePrise{i}"] = candle_list_4h[i][4]
        dataset_4h[f"4H_volume{i}"] = candle_list_4h[i][5]
        dataset_4h[f"4H_turnover{i}"] = candle_list_4h[i][6]
    dataset_1h={}
    for i in range(1000):
        dataset_1h[f"1H_startTime{i}"] = candle_list_1h[i][0]
        dataset_1h[f"1H_openPrice{i}"] = candle_list_1h[i][1]
        dataset_1h[f"1H_highPrice{i}"] = candle_list_1h[i][2]
        dataset_1h[f"1H_lowPrice{i}"] = candle_list_1h[i][3]
        dataset_1h[f"1H_closePrise{i}"] = candle_list_1h[i][4]
        dataset_1h[f"1H_volume{i}"] = candle_list_1h[i][5]
        dataset_1h[f"1H_turnover{i}"] = candle_list_1h[i][6]
    dataset_1m={}
    for i in range(1000):
        dataset_1m[f"1M_startTime{i}"] = candle_list_1m[i][0]
        dataset_1m[f"1M_openPrice{i}"] = candle_list_1m[i][1]
        dataset_1m[f"1M_highPrice{i}"] = candle_list_1m[i][2]
        dataset_1m[f"1M_lowPrice{i}"] = candle_list_1m[i][3]
        dataset_1m[f"1M_closePrise{i}"] = candle_list_1m[i][4]
        dataset_1m[f"1M_volume{i}"] = candle_list_1m[i][5]
        dataset_1m[f"1M_turnover{i}"] = candle_list_1m[i][6]
    dataset_FND={}
    for i in range(200):
        dataset_FND[f"funding_rate{i}"] = fund_rate[i]['fundingRate']
        dataset_FND[f"FR-timestap{i}"] = fund_rate[i]['fundingRateTimestamp']
    dataset_LD={}
    if last_deal == "sell":    
        dataset_LD["last_sell"] = 1
        dataset_LD["last_wait"] = 0
        dataset_LD["last_buy"]=0
    elif last_deal == "buy":
        dataset_LD["last_sell"] = 0
        dataset_LD["last_wait"] = 0
        dataset_LD["last_buy"]=1
    else:
        dataset_LD["last_sell"] = 0
        dataset_LD["last_wait"] = 1
        dataset_LD["last_buy"]=0
    dataset_ND={}
    if what_to_do_now == "sell":    
        dataset_ND["now_sell"] = 1
        dataset_ND["now_wait"] = 0
        dataset_ND["now_buy"]=0
    elif what_to_do_now == "buy":
        dataset_ND["now_sell"] = 0
        dataset_ND["now_wait"] = 0
        dataset_ND["now_buy"]= 1
    else:
        dataset_ND["now_sell"] = 0
        dataset_ND["now_wait"] = 1
        dataset_ND["now_buy"]=0
    dataset_symb={}
    if symbol=="BTCUSDT":
        dataset_symb["symb_type"]=1
    elif symbol=="ETHUSDT":
        dataset_symb["symb_type"]=2
    else:
        dataset_symb["symb_type"]=3
    

    datasets = [dataset_1h, dataset_1m, dataset_4h, dataset_FND, dataset_LD, dataset_ND, dataset_symb]
    dataframes=[]
    for dataset in datasets:
        dataframes.append(pd.DataFrame(dataset, index=[0]))
    return dataframes