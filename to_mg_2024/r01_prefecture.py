import psycopg2
from configparser import ConfigParser
import pymongo
import datetime
config_parser = ConfigParser()
config_parser.read('../config.cfg')
config = config_parser['RAILAROUND']
pg_database = config['pg_database']
pg_hostname = config['pg_hostname']
pg_username = config['pg_username']
pg_password = config['pg_password']
pg_tb_prefecture = config['pg_tb_prefecture']
mg_tb_prefecture = config['mg_tb_prefecture']
last_update = config['last_update']

pgConnection = psycopg2.connect(
    host=pg_hostname, user=pg_username, password=pg_password, dbname=pg_database)
cur = pgConnection.cursor()

# t = datetime.datetime.now()
# format_string = "%y%m%d"
# last_update = t.strftime(format_string)


def r01_prefecture(session, mgdb):
    qry = "SELECT prefecture_ja, prefecture_en, prefecture_zh, area_ja, area_en, area_zh, geo_order, last_update, valid from " + \
        pg_tb_prefecture+" where last_update>="+last_update
    cur.execute(qry)
    results = cur.fetchall()
    insertList = []
    for each in results:
        insertList.append({'prefecture_ja': each[0], 'prefecture_en': each[1], 'prefecture_zh': each[2], 'area_ja': each[3],
                           'area_en': each[4], 'area_zh': each[5], 'geo_order': each[6], 'last_update': each[7], 'valid': each[8]})
    print("新写入"+str(len(insertList))+"条prefecture记录")
    if len(insertList) > 0:
        mgdb[mg_tb_prefecture].insert_many(insertList, session=session)
