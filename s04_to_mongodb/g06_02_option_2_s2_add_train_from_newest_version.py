import configparser
import psycopg2
from configparser import ConfigParser
import unicodedata
import subprocess
import pymongo
config_parser = ConfigParser()
config_parser.read('../config.cfg')
config = config_parser['DEFAULT']
database = config['database']+'_'+config['version_code']
hostname = config['hostname']
username = config['username']
password = config['password']

mongodb_user = config['mongodb_user']
mongodb_password = config['mongodb_password']
mongodb_database = config['mongodb_database']
myclient = pymongo.MongoClient('mongodb+srv://'+mongodb_user+':'+mongodb_password +
                               '@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority')
mydb = myclient[mongodb_database]
new_train = 'train_'+config['version_code']
mycol = mydb[new_train]
tb_to_mongodb_train = 'to_mongodb_train_'+config['version_code']

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()

qry = "SELECT stations_ja, stations_en, stations_zh, detail_info, runday,  dep, arr, route_id, railwaylines, serviceroutes,  train_type, depm, arrm,  icon_image  from  " + \
    tb_to_mongodb_train+" where route_id like '%宇都宮%' "
cur.execute(qry)
results = cur.fetchall()
addtrainlist = []
for each in results:
    addtrainlist.append(
        {"stations_ja": each[0], "stations_en": each[1], "stations_zh": each[2], "detail_info": each[3], "runday": each[4], "dep": each[5], "arr": each[6], "route_id": each[7], "railwaylines": each[8], "serviceroutes": each[9], "train_type": each[10], "depm": each[11], "arrm": each[12], "icon_image": each[13]})


mycol.insert_many(addtrainlist)
