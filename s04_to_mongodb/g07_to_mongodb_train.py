import psycopg2
from configparser import ConfigParser
import pymongo
import os


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
new_train = 'train_'+config['train_version']
tb_to_mongodb_train = 'to_mongodb_train_'+config['train_version']
tb_train = 'train'
tb_to_mongodb_ctl_stations = 'to_mongodb_ctl_stations_' + \
    config['version_code']

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_to_mongodb_train_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_mongodb_train
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_mongodb_train + \
        " as select stations as stations_ja, detail_info, runday, dep, arr from "+tb_train
    cur.execute(qry)
    connection.commit()

    qry = "ALTER TABLE "+tb_to_mongodb_train + "  ADD COLUMN IF NOT EXISTS stations_id text,  ADD COLUMN IF NOT EXISTS railwaylines text[], ADD COLUMN IF NOT EXISTS serviceroutes text[], ADD COLUMN IF NOT EXISTS depm integer, ADD COLUMN IF NOT EXISTS arrm integer"
    cur.execute(qry)
    connection.commit()


def update_from_ctl_stations():

    qry = "Drop INDEX IF EXISTS idx_train; CREATE INDEX idx_train ON " + \
        tb_to_mongodb_train+" (stations_ja);"
    print(qry)
    cur.execute(qry)
    connection.commit()
    qry = "update "+tb_to_mongodb_train + \
        " a set stations_id=b.stations_id, railwaylines=b.railwaylines from " + \
        tb_to_mongodb_ctl_stations+" b where a.stations_ja=b.stations_ja"
    print(qry)
    cur.execute(qry)
    connection.commit()


def update_service_route():
    qry = "update "+tb_to_mongodb_train + \
        " a set serviceroutes=b.service_route from ctl_stations b where a.stations_ja=b.stations"
    print(qry)
    cur.execute(qry)
    connection.commit()


def update_dep_arr():
    qry = "Drop INDEX IF EXISTS idx_train; CREATE INDEX idx_train ON " + \
        tb_to_mongodb_train+" ((dep[1]));"
    print(qry)
    cur.execute(qry)
    connection.commit()

    qry = "select distinct(dep[1]) from " + \
        tb_to_mongodb_train
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        dep = each[0][0:5]
        dep_hour = int(dep.split(":")[0])
        dep_min = int(dep.split(":")[1])
        depm = dep_hour*60+dep_min
        qry = "update "+tb_to_mongodb_train+" set depm="+str(
            depm)+" where dep[1]='"+dep+"'"
        print(qry)
        cur.execute(qry)
        connection.commit()

    qry = "Drop INDEX IF EXISTS idx_train; CREATE INDEX idx_train ON " + \
        tb_to_mongodb_train+" ((arr[array_upper(arr, 1)]));"
    print(qry)
    cur.execute(qry)
    connection.commit()

    qry = "select distinct(arr[array_upper(arr, 1)]) from " + \
        tb_to_mongodb_train
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        arr = each[0][0:5]
        arr_hour = int(arr.split(":")[0])
        arr_min = int(arr.split(":")[1])
        arrm = arr_hour*60+arr_min
        qry = "update "+tb_to_mongodb_train+" set arrm="+str(
            arrm)+" where arr[array_upper(arr, 1)]='"+arr+"'"
        print(qry)
        cur.execute(qry)
        connection.commit()


def update_icon_image():
    qry = "ALTER TABLE "+tb_to_mongodb_train + \
        "  ADD COLUMN IF NOT EXISTS icon_image text"
    cur.execute(qry)
    connection.commit()

    qry = "Drop INDEX IF EXISTS idx_train_detail_info; CREATE INDEX idx_train_detail_info ON " + \
        tb_to_mongodb_train+" ((detail_info[1]));"
    cur.execute(qry)
    connection.commit()

    dict_ctl_train_icon = {}
    qry = "select icon_image, detail from ctl_train_icon"
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        icon_image = each[0]
        detail = each[1]
        dict_ctl_train_icon[detail] = icon_image

    qry = "select distinct(detail_info[1]) from " + tb_to_mongodb_train
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        detail = each[0]
        icon_image = ''
        for d in dict_ctl_train_icon:
            if d in detail:
                icon_image = dict_ctl_train_icon[d]
        if icon_image == '':
            icon_image = detail[0]
        qry = "update "+tb_to_mongodb_train+" set icon_image='"+icon_image + \
            "' where detail_info[1]='" + detail+"'"
        print(qry)
        cur.execute(qry)
        connection.commit()


def upload_to_mongo():
    # 如果不存在这张表，后面将直接新建，如果已存在，此处会将期document全部删除
    mydb[new_train].delete_many({})
    qry = "SELECT stations_ja, stations_id, detail_info, runday, dep, arr, railwaylines, serviceroutes,  depm, arrm, icon_image from (select *  from " + \
        tb_to_mongodb_train+" ) as a"
    print(qry)
    cur.execute(qry)
    results = cur.fetchall()
    with open(os.getcwd()+'/test.json', 'w') as f:
        i = 0
        while i < len(results):
            if divmod(i, 10000)[1] == 0:  # 除以1万，余数为0
                addTrainList = []
                print("new addTrainList")
            addTrainList.append({'stations_ja': results[i][0], 'stations_id': results[i][1],
                                 'detail_info': results[i][2],  'runday': results[i][3], 'dep': results[i][4], 'arr': results[i][5],  'railwaylines': results[i][6], 'serviceroutes': results[i][7], 'depm': results[i][8], 'arrm': results[i][9], 'icon_image': results[i][10]})

            if divmod(i, 10000)[1] == 9999 or i == len(results)-1:  # 除以1千，余数为999,或者最后一个
                mydb[new_train].insert_many(addTrainList)
                print("add another 10000 trains")
                print(i)

            i += 1

    myclient.close()


create_new_to_mongodb_train_table()
update_from_ctl_stations()
update_service_route()
update_dep_arr()
update_icon_image()
upload_to_mongo()
