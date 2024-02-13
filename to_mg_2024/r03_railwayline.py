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
pg_tb_railwayline = config['pg_tb_railwayline']
# pg_wp_railwayline = config['pg_wp_railwayline']
mg_tb_railwayline = config['mg_tb_railwayline']

pg_tb_station = config['pg_tb_station']
last_update = config['last_update']

pgConnection = psycopg2.connect(
    host=pg_hostname, user=pg_username, password=pg_password, dbname=pg_database)
cur = pgConnection.cursor()


def r03_railwayline(session, mgdb):

    # qry = "drop table if exists "+pg_wp_railwayline
    # cur.execute(qry)
    # pgConnection.commit()

    # qry = "create table "+pg_wp_railwayline+" as select railway_line_ja, railway_line_en, railway_line_zh, ra_keys, points, company, type, priority, last_update, valid from " + \
    #     pg_tb_railwayline+" where last_update>="+last_update
    # cur.execute(qry)
    # pgConnection.commit()
    dict_station = {}
    qry = "select name_ja, prefecture from "+pg_tb_station
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        name = each[0]
        prefecture = each[1]
        dict_station[name] = prefecture

    qry = "SELECT railway_line_ja, railway_line_en, railway_line_zh, ra_keys, points, company, type, priority, last_update, valid from " + \
        pg_tb_railwayline+" where last_update>="+last_update
    cur.execute(qry)
    print(qry)
    results = cur.fetchall()
    insertList = []
    for each in results:
        points = each[4]
        prefectures = []
        for p in points:
            prefecture = dict_station[p]
            if prefecture not in prefectures:
                prefectures.append(prefecture)

        insertList.append({'railwayline_ja': each[0], 'railwayline_en': each[1], 'railwayline_zh': each[2], 'ra_keys': each[3],
                           'points': each[4], 'company': each[5], 'type': each[6], 'priority': each[7], 'prefectures': prefectures, 'last_update': each[8], 'valid': each[9]})
    print("新写入"+str(len(insertList))+"条railwayline记录")
    if len(insertList) > 0:
        mgdb[mg_tb_railwayline].insert_many(insertList, session=session)
