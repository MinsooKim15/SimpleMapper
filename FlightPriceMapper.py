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


df = pd.read_sql_query("SELECT * FROM RawFlightQuotes", conn)
recentWriteDate =  df.sort_values("writeDate", ascending = False)["writeDate"].iloc[1,]
pd.to_datetime(recentWriteDate)
recentWriteDate = recentWriteDate.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
today = pd.to_datetime(datetime.now()).replace(hour = 0, minute = 0, second = 0, microsecond = 0)
minDepartureDay = today + pd.Timedelta("173days")
maxDepartureDay = today + pd.Timedelta("188days")#미만이라서 +1일
print("OKAY")

def makeRoundTrip_temp(row):
    #일단은 정확히 +7일만 다루는 코드라서 Temp라고 명명함(수정 예정)
    #TODO : 7일 없으면 뒤로 3일 더 찾도록 변경
    #TODO : 값 조정 가능하도록하자

    targetArrivalDate = row["quoteFirstDepartureDate"] + pd.Timedelta("7days")
    targetArrivalAirport = row["queryDestinationPlace"]
    notGotResult = True
    i = 0
    target = df[(df["queryOriginPlace"] == targetArrivalAirport) & (df["quoteFirstDepartureDate"] == (targetArrivalDate + pd.Timedelta(i, unit='d')))]
    if target.shape[0] == 0:
        return (str(row["quoteFirstDepartureDate"])+"no", np.nan, targetArrivalAirport)
    else:
        roundTripDate = str(row["quoteFirstDepartureDate"]) + "_" + str(targetArrivalDate + pd.Timedelta(i, unit='d'))
        roundTripPrice = row["minPrice"] + target["minPrice"]
        return (roundTripDate, roundTripPrice.tolist()[0],targetArrivalAirport)


def makeDescription(row):
    minPrice = row["flightTodayMinimum"]
    meanPrice = row["flightTodayAverage"]

    return (f"지금의 최저가는 {minPrice}원, 평균 가격은 {meanPrice}원이에요")

round_df = pd.DataFrame()

#대상값 정리 1)인천공항 제거 2)최신 작성일 기준으로 중복제거 3)대상 기간(173 ~ 187일)로 축소
#TODO : 향후 중복 기준을 APICallID로 변경(APICALLID는 한번의 Connector Instance에 대응하도록 변경됨 19.12.28)
cleaned_df = df[df["queryDestinationPlace"] != "ICN"]
cleaned_df = cleaned_df[cleaned_df["writeDate"] > recentWriteDate]
cleaned_df= cleaned_df[(cleaned_df["quoteFirstDepartureDate"] > minDepartureDay) & (cleaned_df["quoteFirstDepartureDate"] < maxDepartureDay)]

round_df = cleaned_df.apply(makeRoundTrip_temp, axis =1, result_type= 'expand')
round_df= round_df.dropna()
round_df = round_df.rename(columns = {0:"roundTripDate", 1:"roundTripPrice", 2:"targetAirport"})
final_df = pd.pivot_table(
    round_df,
    index = "targetAirport",
    values = "roundTripPrice",
    aggfunc = ["min", "mean"]
)
final_df.reset_index(level=0, inplace=True)
final_df.columns = [''.join(col).strip() for col in final_df.columns.values]
final_df = final_df.rename(columns = {"minroundTripPrice" : "flightTodayMinimum", "meanroundTripPrice" : "flightTodayAverage","targetAirport" : "airportId" })
final_df[["flightTodayMinimum", "flightTodayAverage"]] = final_df[["flightTodayMinimum", "flightTodayAverage"]].astype('int')

#남은 Field 채우기
final_df["flightPriceDescription"] = final_df.apply(makeDescription,axis=1)
final_df["flightPriceTitle"] = np.nan
#flightPriceId만들기
final_df["flightPriceId"] = final_df.index + 1
final_df["flightPriceId"] = final_df["flightPriceId"].apply(lambda x : str(x).zfill(4))
final_df["flightPriceId"] = "flightPrice_" + final_df["flightPriceId"] + "_" + str(datetime.now().strftime("%Y%m%d%H%M%S"))
final_df["dateToShow"] = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
final_df["created"] = datetime.today()
final_df["score"] = 100 #점수가 10ㄷㄷ0 점으로 고정되어 있음 수정 필요

engine = create_engine("mysql+pymysql://" +sqlUser +":"+sqlPasswd+"@"+sqlHost + "/" + sqlDb +"?charset=utf8mb4",encoding='utf-8')
conn = engine.connect()
final_df.to_sql(name= "flightPrice", con= engine, if_exists = "append", index = False)
mainLogger.info("SQL Commit Done : FlightPrice " + str(final_df.shape[0]))