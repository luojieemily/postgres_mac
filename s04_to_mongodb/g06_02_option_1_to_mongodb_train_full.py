import configparser
import psycopg2
from configparser import ConfigParser
import unicodedata
import subprocess
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

tb_polygon = 'polygon'
tb_line = 'line'
tb_railway_line = 'railway_line'
tb_ctl_dict_train = 'ctl_stations'
tb_ctl_route_guide = 'ctl_guide'
tb_train = 'train'
tb_service_route = 'service_route'
tb_to_mongodb_train = 'to_mongodb_train_'+config['version_code']

to_file_path = config['temp_file_path']
to_file_name = 'to_mongodb_train_'+config['version_code']+'.json'

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def gen_mongo_train_json():
    qry = "SELECT json_build_object('stations_ja',stations_ja, 'stations_en',stations_en,'stations_zh',stations_zh,'detail_info',detail_info, 'runday',runday, 'dep', dep, 'arr',arr,'route_id', route_id, 'railwaylines',railwaylines,'serviceroutes',serviceroutes, 'train_type', train_type, 'depm', depm, 'arrm', arrm, 'icon_image', icon_image ) from (select * from "+tb_to_mongodb_train+") row"
    cur.execute(qry)
    results = cur.fetchall()
    with open(to_file_path+'/'+to_file_name, 'w') as f:
        f.write('[')
        for row in results:
            f.write(unicodedata.normalize('NFKC', str(
                row[0]).replace("'", '"').replace("None", '""')))
            # 当原数据库里某一列为空时，导出会是None，没有引号，这个导致import to MongoDB出错，需替换为“”

            if row != results[-1]:
                f.write(',')
        # 最后一个object后面不应有逗号
        f.write(']')
        # 导入MongoDB的原始文件需要是一个列表[]

    with open(to_file_path+'/'+to_file_name) as f:
        newText = f.read().replace('"{', '{').replace('}"', '}').replace(
            '"[', '[').replace(']"', ']').replace('True', 'true').replace('False', 'false')

    with open(to_file_path+'/'+to_file_name, 'w') as f:
        f.write(newText)

    subprocess.call('mongoimport --uri "mongodb+srv://'+mongodb_user+':'+mongodb_password+'@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority"  --collection train_' +
                    config['version_code']+' --drop --file ' + to_file_path+'/'+to_file_name+' --jsonArray --maintainInsertionOrder --batchSize 1', shell=True)


gen_mongo_train_json()
