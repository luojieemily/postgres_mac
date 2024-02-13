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
new_line = 'line_'+config['version_code']

tb_polygon = 'polygon'
tb_line = 'line'
tb_service_route = 'service_route'
tb_railway_line = 'railway_line'
tb_to_mongodb_line = 'to_mongodb_line_'+config['version_code']

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_to_mongodb_line_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_mongodb_line
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_mongodb_line + \
        " as select ra_key, ST_SimplifyPreserveTopology(way,5) as way from " + \
        tb_line
    cur.execute(qry)
    connection.commit()

    qry = "ALTER TABLE "+tb_to_mongodb_line + \
        " ADD COLUMN IF NOT EXISTS railwaylines text[], ADD COLUMN IF NOT EXISTS serviceroutes text[], ADD COLUMN IF NOT EXISTS stations text[]"
    cur.execute(qry)
    connection.commit()


def update_cols():
    dict_rk_related_ra_keys = {}
    dict_rk_related_points = {}
    dict_rk_direct_railwaylines = {}
    dict_rk_direct_serviceroutes = {}
    qry = "select ra_keys, points from ctl_guide"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_keys = each[0]
        points = each[1]
        for ra_key in ra_keys:
            if ra_key not in dict_rk_related_ra_keys:
                dict_rk_related_ra_keys[ra_key] = []
            if ra_key not in dict_rk_related_points:
                dict_rk_related_points[ra_key] = []
            for r in ra_keys:
                if r not in dict_rk_related_ra_keys[ra_key]:
                    dict_rk_related_ra_keys[ra_key].append(r)
            for p in points:
                if p not in dict_rk_related_points[ra_key]:
                    dict_rk_related_points[ra_key].append(p)
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

    for ra_key in dict_rk_related_ra_keys:
        stations = dict_rk_related_points[ra_key]
        railwaylines = []
        serviceroutes = []
        for rk in dict_rk_related_ra_keys[ra_key]:
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
        qry = "update "+tb_to_mongodb_line+" set railwaylines=array" + \
            str(railwaylines)+", serviceroutes=array"+str(serviceroutes) + \
            ", stations=array"+str(stations)+" where ra_key='"+ra_key+"'"
        cur.execute(qry)
        connection.commit()


def upload_to_mongo():
    # 如果不存在这张表，后面将直接新建，如果已存在，此处会将期document全部删除
    mydb[new_line].delete_many({})
    qry = "SELECT ra_key, railwaylines, serviceroutes, stations, ST_AsGeoJSON(geog, 8)::jsonb as way  from (select * ,ST_Transform( way, 4326) as geog from " + \
        tb_to_mongodb_line+") as a"
    cur.execute(qry)
    results = cur.fetchall()
    addLineList = []
    for each in results:
        addLineList.append({'ra_key': each[0], 'railwaylines': each[1],
                           'serviceroutes': each[2], 'stations': each[3],  'way': each[4]})

    mydb[new_line].insert_many(addLineList)


create_new_to_mongodb_line_table()
update_cols()
upload_to_mongo()
