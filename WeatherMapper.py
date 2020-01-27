import os
import pymysql
import pandas as pd
from datetime import datetime
import numpy as np

from sqlalchemy import create_engine
import inspect,json

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
with open(path + '/config.json', 'r') as f:
    config = json.load(f)

import logging
mainLogger = logging.getLogger("mainMapper")
mainLogger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
mainLogger.addHandler(streamHandler)
fileHandler = logging.FileHandler(path + "/main.log")
fileHandler.setFormatter(formatter)
mainLogger.addHandler(fileHandler)

sqlHost = config["host"]
sqlUser = config["user"]
sqlPasswd = config["passwd"]
sqlDb = config["db"]

conn = pymysql.connect(
    host= sqlHost,
    port=int(3306),
    user= sqlUser,
    passwd= sqlPasswd,
    db=sqlDb,
    charset='utf8mb4')

weather = pd.read_sql_query("SELECT * FROM rawWeatherGlobalSummary", conn)
seoulWeather = pd.read_sql_query("SELECT * FROM rawWeatherSeoulOnly", conn)
seoulWeather = seoulWeather[seoulWeather.temperatureMax != -999.0]
seoulToday = seoulWeather.sort_values("writeDate", ascending=False)[['temperature']].iloc[0,][0]

weather["month"] = weather["measureDate"].dt.month
weather["year"] = weather["measureDate"].dt.year

summerCountryList = [
    "ID", #인도네시아
    "TH", #태국
    "VM", #베트남
    "MY", #말레이시아
    "LA", #라오스
    "SG", #싱가포르
]
summerStateList = [
    "HI", #하와이
    "GU" #괌
]
summer_df = weather[weather["country"].isin(summerCountryList)]
summer_df = summer_df.append(weather[weather["state"].isin(summerStateList)])
# 데이터를 정리 정돈
summer_df_pv = pd.pivot_table(
    summer_df,
    values = ['coolingDegreeDays', 'heatingDegreeDays',
       'daysMaxTempUnder00', 'daysMaxTempOver21', 'daysMaxTempOver32',
       'daysMaxTempUnderMinus17', 'daysMinTempUnder00',
       'daysPrecipitationOver00', 'daysPrecipitationOver12',
       'daysPrecipitationOver25', 'highestDailyPrecipitation',
       'highestDailySnowDepth', 'highestDailySnowFall', 'daysSnowDepthOver25',
       'daysSnowFallOver25', 'totalMonthlyPrecipitation',
       'totalMonthlySnowfall', 'maxTempMonth', 'minTempMonth',
       'averageMaxTemp', 'averageMinTemp', 'averageMonthlyTemp',
       'averageWindSpeed'],
    index = ["country","state","month"],
    aggfunc = "mean"
)
#고온 조건
summer_df_pv["highTempPenalty"] = (summer_df_pv["averageMaxTemp"] - 31).clip(0).fillna(0)
summer_df_pv["highTempPenalty"] += (summer_df_pv["averageMonthlyTemp"] - 28).clip(0).fillna(0)
summer_df_pv["highTempPenalty"] += (summer_df_pv["daysMaxTempOver32"] - 10).clip(0).fillna(0)
summer_df_pv["highTempPenalty"] += (summer_df_pv["maxTempMonth"] - 34).clip(0).fillna(0)
#저온 조건
summer_df_pv["lowTempPenalty"] = (16 - summer_df_pv["averageMinTemp"]).clip(0).fillna(0)
summer_df_pv["lowTempPenalty"] += (20 - summer_df_pv["averageMonthlyTemp"]).clip(0).fillna(0)
summer_df_pv["lowTempPenalty"] += (summer_df_pv["daysMinTempUnder00"] - 1).clip(0).fillna(0)
summer_df_pv["lowTempPenalty"] += (13 - summer_df_pv["minTempMonth"]).clip(0).fillna(0)
#강우 조건
summer_df_pv["rainPenalty"] = (summer_df_pv["daysPrecipitationOver25"] - 10).clip(0).fillna(0)
summer_df_pv["rainPenalty"] += (summer_df_pv["highestDailyPrecipitation"] - 34).clip(0).fillna(0)
summer_df_pv["rainPenalty"] += (summer_df_pv["totalMonthlyPrecipitation"] - 120).clip(0).fillna(0)
summer_df_pv["rainPenalty"] = summer_df_pv["rainPenalty"]/50 #스케일

#강설 조건
summer_df_pv["snowPenalty"] = (summer_df_pv["daysSnowDepthOver25"] - 0).clip(0).fillna(0)
summer_df_pv["snowPenalty"] += (summer_df_pv["daysSnowFallOver25"] - 0).clip(0).fillna(0)
summer_df_pv["snowPenalty"] += (summer_df_pv["highestDailySnowDepth"] - 0).clip(0).fillna(0)
summer_df_pv["snowPenalty"] += (summer_df_pv["highestDailySnowFall"] - 0).clip(0).fillna(0)
summer_df_pv["snowPenalty"] += (summer_df_pv["totalMonthlySnowfall"] - 0).clip(0).fillna(0)
summer_df_pv["snowPenalty"] = summer_df_pv["snowPenalty"]/50 #스케일

normal_df = weather[~((weather["country"].isin(summerCountryList))|(weather["state"].isin(summerStateList)))]

# 데이터를 정리 정돈
normal_df_pv = pd.pivot_table(
    normal_df,
    values = ['coolingDegreeDays', 'heatingDegreeDays',
       'daysMaxTempUnder00', 'daysMaxTempOver21', 'daysMaxTempOver32',
       'daysMaxTempUnderMinus17', 'daysMinTempUnder00',
       'daysPrecipitationOver00', 'daysPrecipitationOver12',
       'daysPrecipitationOver25', 'highestDailyPrecipitation',
       'highestDailySnowDepth', 'highestDailySnowFall', 'daysSnowDepthOver25',
       'daysSnowFallOver25', 'totalMonthlyPrecipitation',
       'totalMonthlySnowfall', 'maxTempMonth', 'minTempMonth',
       'averageMaxTemp', 'averageMinTemp', 'averageMonthlyTemp',
       'averageWindSpeed'],
    index = ["country", "state","month"],
    aggfunc = "mean"
)
#고온 조건
normal_df_pv["highTempPenalty"] = (normal_df_pv["averageMaxTemp"] - 24).clip(0).fillna(0)
normal_df_pv["highTempPenalty"] += (normal_df_pv["averageMonthlyTemp"] - 18).clip(0).fillna(0)
normal_df_pv["highTempPenalty"] += (normal_df_pv["daysMaxTempOver32"] - 12).clip(0).fillna(0)
normal_df_pv["highTempPenalty"] += (normal_df_pv["maxTempMonth"] - 31).clip(0).fillna(0)
normal_df_pv["highTempPenalty"] = normal_df_pv["highTempPenalty"]/2
#저온 조건
normal_df_pv["lowTempPenalty"] = (12 - normal_df_pv["averageMinTemp"]).clip(0).fillna(0)
normal_df_pv["lowTempPenalty"] += (12 - normal_df_pv["averageMonthlyTemp"]).clip(0).fillna(0)
normal_df_pv["lowTempPenalty"] += (normal_df_pv["daysMinTempUnder00"] - 25).clip(0).fillna(0)
normal_df_pv["lowTempPenalty"] += (0 - normal_df_pv["minTempMonth"]).clip(0).fillna(0)
normal_df_pv["lowTempPenalty"] = normal_df_pv["lowTempPenalty"]

#강우 조건
normal_df_pv["rainPenalty"] = (normal_df_pv["daysPrecipitationOver25"] - 16).clip(0).fillna(0)
normal_df_pv["rainPenalty"] += (normal_df_pv["highestDailyPrecipitation"] - 34).clip(0).fillna(0)
normal_df_pv["rainPenalty"] += (normal_df_pv["totalMonthlyPrecipitation"] - 108).clip(0).fillna(0)
normal_df_pv["rainPenalty"] = normal_df_pv["rainPenalty"]/100 #스케일
#강설 조건
normal_df_pv["snowPenalty"] = (normal_df_pv["daysSnowDepthOver25"] - 17).clip(0).fillna(0)
normal_df_pv["snowPenalty"] += (normal_df_pv["daysSnowFallOver25"] - 3).clip(0).fillna(0)
normal_df_pv["snowPenalty"] += (normal_df_pv["highestDailySnowDepth"] - 2).clip(0).fillna(0)
normal_df_pv["snowPenalty"] += (normal_df_pv["highestDailySnowFall"] - 14).clip(0).fillna(0)
normal_df_pv["snowPenalty"] += (normal_df_pv["totalMonthlySnowfall"] - 21).clip(0).fillna(0)
normal_df_pv["snowPenalty"] = normal_df_pv["snowPenalty"]/100 #스케일
result_df = normal_df_pv.append(summer_df_pv)
result_df["score"] = 100 - result_df["highTempPenalty"] - result_df["lowTempPenalty"] - result_df["rainPenalty"] - result_df["snowPenalty"]
del weather
del seoulWeather
del summer_df_pv
del normal_df_pv
result_df.loc[(result_df["score"] >= 80),"reasonBad"] = "good"
#조건의 역순으로 데이터를 채운다.
result_df.loc[(result_df["score"] < 80),"reasonBad"] = "justNotGood"
#tempLow, tempHigh, snow, rain순으로 올라가자
result_df.loc[((result_df["score"] < 80) & (result_df["lowTempPenalty"] >= 10)),"reasonBad"] = "tempLow"
result_df.loc[((result_df["score"] < 80) & (result_df["highTempPenalty"] >= 10)),"reasonBad"]= "tempHigh"
result_df.loc[((result_df["score"] < 80) & (result_df["snowPenalty"] >= 10)),"reasonBad"] = "snow"
result_df.loc[((result_df["score"] < 80) & (result_df["rainPenalty"] >= 10)),"reasonBad"] = "rain"

result_df["seoulToday"] = seoulToday
#데이터를 너무 예상할 수 없어서 거대한 평균을 떄립니다.
result_df["mean_tmp"] = ((result_df[['averageMaxTemp','averageMinTemp']].mean(axis=1)))
result_df["placeAverage"] = ((result_df[['mean_tmp','averageMonthlyTemp']].mean(axis=1)))

result_df["compareTempSeoul"] = "similar"
result_df.loc[((result_df["seoulToday"] - result_df["placeAverage"]) > 3),"compareTempSeoul"] = "lowerHere"
result_df.loc[((result_df["seoulToday"] - result_df["placeAverage"]) < -3),"compareTempSeoul"] = "higherHere"

result_df = result_df.rename(columns={"daysPrecipitationOver00": "rainDays"})
result_df.reset_index(inplace=True)
result_df = result_df[result_df["month"] == (datetime.now().month + 1)]
result_df = result_df.filter(items = ["country","state","month","score", "reasonBad", "compareTempSeoul", "rainDays", "placeAverage", "seoulToday", "lowTempPenalty", "highTempPenalty", "snowPenalty", "rainPenalty"])

## 이 뒤는 문장 생성기
result_df["sentence1"] = "한 달 뒤는"

result_df["sentence2"] = "가장 즐기기 좋은 시기에요."
result_df.loc[(result_df["score"] < 80)&(result_df["reasonBad"] == "justNotGood") ,"sentence2"] = "여행하기 조금 불편한 날씨에요."
result_df.loc[(result_df["score"] < 80)&(result_df["reasonBad"] == "tempLow") ,"sentence2"] = "많이 추워 여행하기 불편한 시기에요."
result_df.loc[(result_df["score"] < 80)&(result_df["reasonBad"] == "tempHigh") ,"sentence2"] = "많이 더워 여행하기 불편한 시기에요."
result_df.loc[(result_df["score"] < 80)&(result_df["reasonBad"] == "snow") ,"sentence2"] = "눈이 많이 와 조금 불편한 시기에요."
result_df.loc[(result_df["score"] < 80)&(result_df["reasonBad"] == "rain") ,"sentence2"] = "비가 많이 와 조금 불편한 시기에요."
result_df.loc[result_df["score"] >= 80 ,"sentence2"] = "여행하기 적당한 시기에요."
result_df.loc[result_df["score"] >= 90 ,"sentence2"] = "가장 즐기기 좋은 시기에요."

result_df["sentence3"] = "지금의 한국 기온과 비슷해요"
result_df.loc[(result_df["compareTempSeoul"] == "similar"),"sentence3"] = "지금의 한국 기온과 비슷해요"
result_df.loc[(result_df["compareTempSeoul"] == "lowerHere"),"sentence3"] = "지금의 한국 기온보다는 낮아요."
result_df.loc[(result_df["compareTempSeoul"] == "higherHere"),"sentence3"] = "지금의 한국 기온보다는 높아요."

result_df["description"] = result_df["sentence1"] + " " + result_df["sentence2"] + " " + result_df["sentence3"]
result_df["title"] = "날씨"

result_df["rainDays"] = result_df["rainDays"].fillna(0)

result_df["created"] = datetime.today()
result_df["dateToShow"] = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
result_df["weatherId"] = result_df.index + 1
result_df["weatherId"] = result_df["weatherId"].apply(lambda x : str(x).zfill(4))
result_df["weatherId"] = "weather_" + result_df["weatherId"]  + "_" + str(datetime.now().strftime("%Y%m%d%H%M%S"))

result_df = result_df.drop(columns=['sentence1', 'sentence2', 'sentence3','month'])


# 실서비스용 필드는 Int 처리
result_df["rainDays"] = result_df["rainDays"].astype('int',errors='ignore')
result_df["score"] = result_df["score"].astype('int',errors='ignore')
result_df["placeAverage"] = result_df["placeAverage"].astype('int',errors='ignore')

engine = create_engine("mysql+pymysql://" +sqlUser +":"+sqlPasswd+"@"+sqlHost + "/" + sqlDb +"?charset=utf8mb4",encoding='utf-8')
conn = engine.connect()
result_df.to_sql(name= "weather", con= engine, if_exists = "append",index = False)
mainLogger.info("SQL Commit Done : Weather " + str(result_df.shape[0]))
