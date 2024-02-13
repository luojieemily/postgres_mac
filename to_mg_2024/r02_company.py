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
pg_tb_company = config['pg_tb_company']
mg_tb_company = config['mg_tb_company']
last_update = config['last_update']

pgConnection = psycopg2.connect(
    host=pg_hostname, user=pg_username, password=pg_password, dbname=pg_database)
cur = pgConnection.cursor()


def r02_company(session, mgdb):
    qry = "SELECT company_ja, company_en, company_zh, last_update, valid from " + \
        pg_tb_company+" where last_update>="+last_update
    cur.execute(qry)
    results = cur.fetchall()
    insertList = []
    for each in results:
        insertList.append({'company_ja': each[0], 'company_en': each[1],
                          'company_zh': each[2],  'last_update': each[3], 'valid': each[4]})
    print("新写入"+str(len(insertList))+"条company记录")
    if len(insertList) > 0:
        mgdb[mg_tb_company].insert_many(insertList, session=session)
