#-*- coding:utf-8 -*-
import os
import sys
import io

import pymysql
import pandas as pd
import numpy as np
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import inspect,json

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
with open(path + '/config.json', 'r') as f:
    config = json.load(f)

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

exchange = pd.read_sql_query("SELECT * FROM rawExchange", conn)
print("OKAY")

# 중요 함수(Apply 또는 Agg에서 사용할 거)
# 이동 평균 구하기
def mv5(row):
    return row.nlargest(5,["rateDate"]).mean()
def mv10(row):
    return row.nlargest(10,["rateDate"]).mean()
def mv20(row):
    return row.nlargest(20,["rateDate"]).mean()
def mv60(row):
    return row.nlargest(60,["rateDate"]).mean()
def mv500(row):
    return row.nlargest(500,["rateDate"]).mean()

# 특정 시점 값 구하기 환율 별로 Null 값이 있어서, 실제 값 기준으로 순서를 셈
def getRecent(row):
    return row.sort_values('rateDate').tail(1)["basicRate(clean)"].tolist()[0]
def getWeekAgo(row):
    return row.sort_values('rateDate').tail(5)["basicRate(clean)"].tolist()[0]
def getMonthAgo(row):
    return row.sort_values('rateDate').tail(20)["basicRate(clean)"].tolist()[0]

# 문장 작성 코드 -> 나중에 Json 작성 운영툴로 변경하자
def makeFirstSentence(row):
    #전체 Set을 받아서 내가 원하는 Column을 뽑을 거
    if row["mv500"] > row["todayRate"]:
        return u"환율이 낮은 편이에요."
    elif row["mv500"] < row["todayRate"]:
        return u"환율이 높은 편이에요."
    else: #환율이 지난 500일 이평과 같다.
        return u"환율이 적절해요."
def makeSecondSentence(row):
    if row["mv5"] > row["mv10"]:
        if row["mv10"] > row["mv60"]:
            return u"환율이 계속 오르고 있어요."
        else :
            return u"환율이 최근 올랐어요."
    elif row["mv5"] < row["mv10"]:
        if row["mv10"] < row["mv60"]:
            return u"환율이 계속 내리고 있어요."
        else :
            return u"환율이 최근 내렸어요."
    else:
        return u"환율이 큰 변화가 없어요."



# Data Cleansing
exchange["basicRate(clean)"] = pd.to_numeric(exchange["basicRate"].str.replace(',',''), errors= 'coerce')

# 이동평균 구하기
new_df = pd.DataFrame()
new_df["mv5"] = exchange.groupby(["currencyUnit"])["basicRate(clean)","rateDate"].agg(mv5)["basicRate(clean)"]
new_df["mv10"] = exchange.groupby(["currencyUnit"])["basicRate(clean)","rateDate"].agg(mv10)["basicRate(clean)"]
new_df["mv20"] = exchange.groupby(["currencyUnit"])["basicRate(clean)","rateDate"].agg(mv20)["basicRate(clean)"]
new_df["mv60"] = exchange.groupby(["currencyUnit"])["basicRate(clean)","rateDate"].agg(mv60)["basicRate(clean)"]
new_df["mv500"] = exchange.groupby(["currencyUnit"])["basicRate(clean)","rateDate"].agg(mv500)["basicRate(clean)"]
#아직 안 쓸 수 있지만, 쓸 수도 있어 유지하는 코드
new_df["recentNum"] = new_df["mv5"] - new_df["mv10"]
new_df["monthNum"] = new_df["mv10"] - new_df["mv20"]
new_df["quarterNum"] = new_df["mv20"] - new_df["mv60"]

new_df["todayRate"] = exchange.groupby("currencyUnit")[["basicRate(clean)", "rateDate"]].agg(getRecent)["rateDate"]
new_df["weekAgoRate"] = exchange.groupby("currencyUnit")[["basicRate(clean)", "rateDate"]].agg(getWeekAgo)["rateDate"]
new_df["monthAgoRate"] = exchange.groupby("currencyUnit")[["basicRate(clean)", "rateDate"]].agg(getMonthAgo)["rateDate"]

new_df["1stSentence"] = new_df[["mv500", "todayRate"]].apply(makeFirstSentence, axis=1)
new_df["2ndSentence"] = new_df[["mv5", "mv10", "mv60"]].apply(makeSecondSentence, axis=1)
new_df = new_df.reset_index(level='currencyUnit')

final_df = new_df
final_df["rateTitle"] = np.nan
final_df["rateDescription"] = final_df["1stSentence"] + " " + final_df["2ndSentence"]
# final_df["rateDescription"] = final_df["rateDescription"].str.encode('utf-8')
final_df = final_df.drop(["mv5", "mv10", "mv20", "mv60", "mv500","recentNum", "monthNum", "quarterNum", "1stSentence", "2ndSentence"],axis=1)
currencyMap = pd.Series(exchange.currencyName.values, index = exchange.currencyUnit).to_dict()
final_df["currencyName"] =  final_df["currencyUnit"].map(currencyMap)
final_df["created"] = datetime.today()
final_df["dateToShow"] = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
final_df["exchangeId"] = final_df.index + 1
final_df["exchangeId"] = final_df["exchangeId"].apply(lambda x : str(x).zfill(4))
final_df["exchangeId"] = "exchange_" + final_df["exchangeId"]  + "_" + str(datetime.now().strftime("%Y%m%d%H%M%S"))
# print(final_df["rateDescription"])

engine = create_engine("mysql+pymysql://" +sqlUser +":"+sqlPasswd+"@"+sqlHost + "/" + sqlDb +"?charset=utf8mb4",encoding='utf-8')
conn = engine.connect()
final_df.to_sql(name= "exchange", con= engine, if_exists = "append", index = False)
