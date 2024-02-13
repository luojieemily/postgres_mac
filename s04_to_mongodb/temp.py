import psycopg2
from configparser import ConfigParser
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
tb_train = 'train'


connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()

qry = "select * from ctl_prefecture order by geo_order"
cur.execute(qry)
r = cur.fetchall()
areaList = []
prefectureList = []
for each in r:
    prefecture_ja = each[0]
    prefecture_zh = each[1]
    prefecture_en = each[2]
    area_ja = each[3]
    area_zh = each[4]
    area_en = each[5]
    if area_ja not in areaList:
        areaList.append(area_ja)
        prefectureList.append(
            {'area_ja': area_ja, 'area_en': area_en, 'area_zh': area_zh, 'prefecture': [{'prefecture_ja': prefecture_ja, 'prefecture_en': prefecture_en, 'prefecture_zh': prefecture_zh}]})
    else:
        prefecture = prefectureList[-1]['prefecture']
        prefecture.append({'prefecture_ja': prefecture_ja,
                          'prefecture_en': prefecture_en, 'prefecture_zh': prefecture_zh})
        prefectureList[-1]['prefecture'] = prefecture
print(prefectureList)
