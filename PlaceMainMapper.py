import pandas as pd
from datetime import datetime

import os
import pymysql
import pandas as pd

from sqlalchemy import create_engine
import inspect,json

path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
with open(path + '/config.json', 'r') as f:
    config = json.load(f)
import logging

sqlHost = config["host"]
sqlUser = config["user"]
sqlPasswd = config["passwd"]
sqlDb = config["db"]



mainLogger = logging.getLogger("placeMainMapper")
mainLogger.setLevel(logging.INFO)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
mainLogger.addHandler(streamHandler)
fileHandler = logging.FileHandler(path + "/main.log")
fileHandler.setFormatter(formatter)
mainLogger.addHandler(fileHandler)

conn = pymysql.connect(
    host="localhost",
    port=int(3306),
    user="root",
    passwd="1234567890",
    db="eightDays",
    charset='utf8mb4')

placeStatic = pd.read_sql_query("SELECT * FROM placeStatic", conn)
conn.close()
placeStatic["stateName"] = placeStatic["stateName"].fillna("no_state")
placeStatic["stateCode"] = placeStatic["stateCode"].fillna("no_state")

conn = pymysql.connect(
    host="localhost",
    port=int(3306),
    user="root",
    passwd="1234567890",
    db="eightDays",
    charset='utf8mb4')
weather = pd.read_sql_query("SELECT * FROM weather", conn)
conn.close()
targetMonth = datetime.now().month + 1
weather = weather[weather["month"] == targetMonth]
weather = weather.sort_values("created")
weather.drop_duplicates(subset = ["country", "state"],keep = "last",inplace = True)
conn = pymysql.connect(
    host="localhost",
    port=int(3306),
    user="root",
    passwd="1234567890",
    db="eightDays",
    charset='utf8mb4')

exchange = pd.read_sql_query("SELECT * FROM exchange", conn)
conn.close()
exchange= exchange.sort_values("created")
exchange.drop_duplicates(subset = ["currencyUnit"],keep = "last",inplace = True)
exchange['currencyUnit'] = exchange['currencyUnit'].str.slice(0,3)
conn = pymysql.connect(
    host="localhost",
    port=int(3306),
    user="root",
    passwd="1234567890",
    db="eightDays",
    charset='utf8mb4')

flightPrice = pd.read_sql_query("SELECT * FROM flightPrice", conn)
conn.close()
flightPrice = flightPrice.sort_values("created")
flightPrice.drop_duplicates(subset = ["airportId"],keep = "last",inplace = True)
placeStatic = placeStatic.merge(flightPrice[["airportId", "score"]],how= "left", left_on = "airport1",right_on = "airportId").rename(columns = {"score":"flight_score"})
placeStatic = placeStatic.merge(weather[["country","state","score"]], how= 'left', left_on = ["countryCode","stateCode"], right_on = ["country","state"]).rename(columns = {"score":"weather_score"})
placeStatic = placeStatic.merge(exchange[["currencyUnit","score"]], how= 'left', left_on = ["currencyUnit"], right_on = ["currencyUnit"]).rename(columns = {"score":"exchange_score"})
placeStatic["exchange_score"] = placeStatic["exchange_score"].fillna(100)
placeStatic["flight_score"] = placeStatic["flight_score"].fillna(100)
placeStatic["weather_score"] = placeStatic["weather_score"].fillna(placeStatic["weather_score"].mean())
placeStatic["score"] = placeStatic["exchange_score"] * 0.2 + placeStatic["weather_score"] * 0.6 + placeStatic["flight_score"]* 0.2
placeStatic["score"] = placeStatic["score"].astype(int)
final_df = placeStatic
del placeStatic
final_df["placeMainDescId"] = final_df.index + 1
final_df["placeMainDescId"] = final_df["placeMainDescId"].apply(lambda x : str(x).zfill(4))
final_df["placeMainDescId"] = "placeMainDesc_" + final_df["placeMainDescId"]  + "_" + str(datetime.now().strftime("%Y%m%d%H%M%S"))
final_df= final_df.filter(["placeMainDescId", "score", "placeId"])
final_df["created"] = datetime.now()
final_df["dateToShow"] = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

engine = create_engine("mysql+pymysql://" +sqlUser +":"+sqlPasswd+"@"+sqlHost + "/" + sqlDb +"?charset=utf8mb4",encoding='utf-8')
conn = engine.connect()
final_df.to_sql(name= "placeMainDesc", con= engine, if_exists = "append",index = False)
mainLogger.info("SQL Commit Done : PlaceMainDesc " + str(final_df.shape[0]))
