import configparser
import psycopg2
from configparser import ConfigParser
import os
import unicodedata
import json


config_parser = ConfigParser()
config_parser.read('./config.cfg')
config = config_parser['DEFAULT']
database = config['database']+'_'+config['version_code']
hostname = config['hostname']
username = config['username']
password = config['password']
path = "/Users/luojie/MyCode/logo/companyLogo/"


connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()
qry = "select ra_keys, railway_line_ja from railway_line"
cur.execute(qry)
r = cur.fetchall()
for each in r:
    ra_keys = each[0]
    railway_line_ja = each[1]
    for ra_key in ra_keys:
        qry = "update line set railway_line='" + \
            railway_line_ja+"' where ra_key='"+ra_key+"'"
        print(qry)
        cur.execute(qry)
        connection.commit()
