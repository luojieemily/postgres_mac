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

tb_polygon = 'polygon'
tb_line = 'line'
tb_railway_line = 'railway_line'
tb_service_route = 'service_route'
tb_ctl_stations = 'ctl_stations'
tb_to_mongodb_ctl_stations = 'to_mongodb_ctl_stations_' + \
    config['version_code']
myclient = pymongo.MongoClient('mongodb+srv://'+mongodb_user+':'+mongodb_password +
                               '@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority')
mydb = myclient[mongodb_database]
new_ctl_stations = 'ctl_stations_'+config['version_code']

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_to_mongodb_ctl_stations_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_mongodb_ctl_stations
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_mongodb_ctl_stations + \
        " as select stations as stations_ja, isshinkansen, route_id, ra_keys from "+tb_ctl_stations
    cur.execute(qry)
    connection.commit()

    qry = "ALTER TABLE "+tb_to_mongodb_ctl_stations + \
        " ADD COLUMN IF NOT EXISTS railwaylines text[], ADD COLUMN IF NOT EXISTS serviceroutes text[], ADD COLUMN IF NOT EXISTS train_type text,  ADD COLUMN IF NOT EXISTS stations_en text[],  ADD COLUMN IF NOT EXISTS stations_zh text[];"
    cur.execute(qry)
    connection.commit()

    qry = "Drop INDEX IF EXISTS idx_ctl_stations_ja; CREATE INDEX idx_ctl_stations_ja ON " + \
        tb_to_mongodb_ctl_stations+" (stations_ja);"
    cur.execute(qry)
    connection.commit()


def update_cols_from_ctl_stations():
    dict_rk_direct_railwaylines = {}
    dict_rk_direct_serviceroutes = {}
    dict_rk_direct_type = {}
    dict_polygon = {}

    qry = "select ra_key, railway_line from "+tb_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_key = each[0]
        railwayline = each[1]
        if ra_key not in dict_rk_direct_railwaylines:
            dict_rk_direct_railwaylines[ra_key] = []
        if railwayline not in dict_rk_direct_railwaylines[ra_key]:
            dict_rk_direct_railwaylines[ra_key].append(railwayline)

    qry = "select ra_keys, service_route_ja from "+tb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_keys = each[0]
        serviceroute = each[1]
        for ra_key in ra_keys:
            if ra_key not in dict_rk_direct_serviceroutes:
                dict_rk_direct_serviceroutes[ra_key] = []
            if serviceroute not in dict_rk_direct_serviceroutes[ra_key]:
                dict_rk_direct_serviceroutes[ra_key].append(serviceroute)

    qry = "select ra_keys, type from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_keys = each[0]
        the_type = each[1]
        for ra_key in ra_keys:
            dict_rk_direct_type[ra_key] = the_type
    qry = "select name_ja, name_en,name_zh from "+tb_polygon
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        name_ja = each[0]
        name_en = each[1]
        name_zh = each[2]
        dict_polygon[name_ja] = [name_en, name_zh]

    qry = "select stations_ja, isshinkansen,  ra_keys from "+tb_to_mongodb_ctl_stations
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        stations_ja = each[0]
        isshinkansen = each[1]
        ra_keys = each[2]
        dict_type = {}
        railwaylines = []
        serviceroutes = []
        stations_en = []
        stations_zh = []
        for station in stations_ja:
            if station in dict_polygon:
                stations_en.append(dict_polygon[station][0])
                stations_zh.append(dict_polygon[station][1])
            else:
                stations_en.append(station)
                stations_zh.append(station)

        for rk in ra_keys:
            if rk in dict_rk_direct_railwaylines:
                lines = dict_rk_direct_railwaylines[rk]
                for l in lines:
                    if l not in railwaylines:
                        railwaylines.append(l)
            if rk in dict_rk_direct_serviceroutes:
                routes = dict_rk_direct_serviceroutes[rk]
                for r in routes:
                    if r not in serviceroutes:
                        serviceroutes.append(r)
            if rk in dict_rk_direct_type:
                the_type = dict_rk_direct_type[rk]
                if the_type not in dict_type:
                    dict_type[the_type] = 0
                dict_type[the_type] += 1
        if isshinkansen:
            train_type = 'shinkansen'
        else:
            max_count = 0
            train_type = ''
            for t in dict_type:
                if dict_type[t] > max_count:
                    train_type = t
                    max_count = dict_type[t]
        qry = "update "+tb_to_mongodb_ctl_stations+" set railwaylines=array" + \
            str(railwaylines)+", serviceroutes=array"+str(serviceroutes) + \
            ", train_type='"+train_type + \
            "',  stations_en=array" + \
            str(stations_en)+", stations_zh=array"+str(stations_zh) + \
            " where stations_ja=array"+str(stations_ja)
        print(qry)
        cur.execute(qry)
        connection.commit()


def upload_to_mongo():
    # 如果不存在这张表，后面将直接新建，如果已存在，此处会将期document全部删除
    mydb[new_ctl_stations].delete_many({})
    qry = "SELECT stations_id, stations_ja, stations_en, stations_zh, isshinkansen, route_id, ra_keys, railwaylines, serviceroutes, train_type from (select *  from " + \
        tb_to_mongodb_ctl_stations+") as a"
    cur.execute(qry)
    results = cur.fetchall()
    addCtlStationsList = []
    for each in results:
        addCtlStationsList.append({'stations_id': each[0], 'stations_ja': each[1],
                                   'stations_en': each[2], 'stations_zh': each[3],  'isshinkansen': each[4], 'route_id': each[5], 'ra_keys': each[6], 'railwaylines': each[7], 'serviceroutes': each[8], 'train_type': each[9]})

    mydb[new_ctl_stations].insert_many(addCtlStationsList)


create_new_to_mongodb_ctl_stations_table()
update_cols_from_ctl_stations()
upload_to_mongo()
