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
tb_ctl_route_guide = 'ctl_guide'
tb_to_mongodb_ctl_route_guide = 'to_mongodb_ctl_route_guide_' + \
    config['version_code']
myclient = pymongo.MongoClient('mongodb+srv://'+mongodb_user+':'+mongodb_password +
                               '@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority')
mydb = myclient[mongodb_database]
new_ctl_guide = 'ctl_guide_'+config['version_code']

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_to_mongodb_ctl_route_guide_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_mongodb_ctl_route_guide
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_mongodb_ctl_route_guide + \
        " as select route_id, ra_keys, points, fbs from "+tb_ctl_route_guide
    cur.execute(qry)
    connection.commit()

    qry = "ALTER TABLE "+tb_to_mongodb_ctl_route_guide + \
        " ADD COLUMN IF NOT EXISTS railwaylines text[], ADD COLUMN IF NOT EXISTS serviceroutes text[]"
    cur.execute(qry)
    connection.commit()


def update_cols():
    dict_rk_direct_railwaylines = {}
    dict_rk_direct_serviceroutes = {}
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

    qry = "select ra_keys, route_id from ctl_guide"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_keys = each[0]
        route_id = each[1]
        railwaylines = []
        serviceroutes = []
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
        qry = "update "+tb_to_mongodb_ctl_route_guide+" set railwaylines=array" + \
            str(railwaylines)+", serviceroutes=array" + \
            str(serviceroutes)+" where route_id='"+route_id+"'"
        cur.execute(qry)
        connection.commit()


def upload_to_mongo():
    # 如果不存在这张表，后面将直接新建，如果已存在，此处会将期document全部删除
    mydb[new_ctl_guide].delete_many({})
    qry = "SELECT route_id, ra_keys, points, fbs, railwaylines, serviceroutes from (select *  from " + \
        tb_to_mongodb_ctl_route_guide+") as a"
    cur.execute(qry)
    results = cur.fetchall()
    addCtlguideList = []
    for each in results:
        addCtlguideList.append({'route_id': each[0], 'ra_keys': each[1],
                                'points': each[2], 'fbs': each[3],  'railwaylines': each[4], 'serviceroutes': each[5]})

    mydb[new_ctl_guide].insert_many(addCtlguideList)


create_new_to_mongodb_ctl_route_guide_table()
update_cols()
upload_to_mongo()
