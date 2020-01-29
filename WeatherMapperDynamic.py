# 고정된 값 입력, 관리를 위해 스크립트 파일로 생성했지만 쓸 일 없음(EC2에서 못 견딤ㅜ)

import os
import pymysql
import pandas as pd
from datetime import datetime
import inspect,json
import logging

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

mainLogger = logging.getLogger("WeatherMapperDynamic")
mainLogger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
mainLogger.addHandler(streamHandler)
fileHandler = logging.FileHandler(path + "/main.log")
fileHandler.setFormatter(formatter)
mainLogger.addHandler(fileHandler)

weather = pd.read_sql_query("SELECT * FROM weather", conn)

seoulWeather = pd.read_sql_query("SELECT * FROM rawWeatherSeoulOnly", conn)
#클렌징을 해야 한다ㅜㅜ
#일단 Temperature에 -999를 넣는 경우가 있네, 날리자
#그걸 날리고 writeDate로 정렬해서 가장 최신을 쓰자
seoulWeather.sort_values("writeDate")
seoulWeather = seoulWeather[seoulWeather.temperatureMax != -999.0]
seoulToday = seoulWeather.sort_values("writeDate", ascending=False)[['temperature']].iloc[0,][0]
weather["seoulToday"] = seoulToday
weather["compareTempSeoul"] = "similar"
weather.loc[((weather["seoulToday"] - weather["placeAverage"]) > 3),"compareTempSeoul"] = "lowerHere"
weather.loc[((weather["seoulToday"] - weather["placeAverage"]) < -3),"compareTempSeoul"] = "higherHere"
weather["sentence1"] = "한 달 뒤는"
weather["sentence2"] = "가장 즐기기 좋은 시기에요."
weather.loc[(weather["score"] < 80)&(weather["reasonBad"] == "justNotGood") ,"sentence2"] = "여행하기 조금 불편한 날씨에요."
weather.loc[(weather["score"] < 80)&(weather["reasonBad"] == "tempLow") ,"sentence2"] = "많이 추워 여행하기 불편한 시기에요."
weather.loc[(weather["score"] < 80)&(weather["reasonBad"] == "tempHigh") ,"sentence2"] = "많이 더워 여행하기 불편한 시기에요."
weather.loc[(weather["score"] < 80)&(weather["reasonBad"] == "snow") ,"sentence2"] = "눈이 많이 와 조금 불편한 시기에요."
weather.loc[(weather["score"] < 80)&(weather["reasonBad"] == "rain") ,"sentence2"] = "비가 많이 와 조금 불편한 시기에요."
weather.loc[weather["score"] >= 80 ,"sentence2"] = "여행하기 적당한 시기에요."
weather.loc[weather["score"] >= 90 ,"sentence2"] = "가장 즐기기 좋은 시기에요."
weather["sentence3"] = "지금의 한국 기온과 비슷해요"
weather.loc[(weather["compareTempSeoul"] == "similar"),"sentence3"] = "지금의 한국 기온과 비슷해요"
weather.loc[(weather["compareTempSeoul"] == "lowerHere"),"sentence3"] = "지금의 한국 기온보다는 낮아요."
weather.loc[(weather["compareTempSeoul"] == "higherHere"),"sentence3"] = "지금의 한국 기온보다는 높아요."
weather["description"] = weather["sentence1"] + " " + weather["sentence2"] + " " + weather["sentence3"]
weather["title"] = "날씨"
weather["rainDays"] = weather["rainDays"].fillna(0)
weather["created"] = datetime.today()
weather["dateToShow"] = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
weather = weather.drop(columns=['sentence1', 'sentence2', 'sentence3'])
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://" +sqlUser +":"+sqlPasswd+"@"+sqlHost + "/" + sqlDb +"?charset=utf8mb4",encoding='utf-8')
conn = engine.connect()
weather.to_sql(name= "weather", con= engine, if_exists = "replace",index = False)
mainLogger.info("SQL Commit Done : Weather " + str(weather.shape[0]))
