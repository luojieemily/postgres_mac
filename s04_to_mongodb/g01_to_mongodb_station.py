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
new_station = 'station_'+config['version_code']

tb_to_mongodb_station = 'to_mongodb_station_'+config['version_code']
tb_polygon = 'polygon'
tb_line = 'line'
tb_railway_line = 'railway_line'
tb_service_route = 'service_route'


connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_to_mongodb_station_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_mongodb_station
    cur.execute(qry)
    connection.commit()
    qry = "CREATE TABLE "+tb_to_mongodb_station + \
        " as select name_ja as station_ja, name_en as station_en, name_zh as station_zh, prefecture as prefecture_ja, st_centroid(way) as way  from " + \
        tb_polygon
    cur.execute(qry)
    connection.commit()


def complete_prefecture():
    qry = "alter table "+tb_to_mongodb_station + \
        " add column IF NOT EXISTS prefecture_ja text, add column IF NOT EXISTS prefecture_en text, add column IF NOT EXISTS prefecture_zh text"
    cur.execute(qry)
    connection.commit()
    qry = "select prefecture_ja, prefecture_en, prefecture_zh from ctl_prefecture"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        prefecture_ja = each[0]
        prefecture_en = each[1]
        prefecture_zh = each[2]

        qry = "update "+tb_to_mongodb_station+" set prefecture_en='"+prefecture_en + \
            "', prefecture_zh='"+prefecture_zh+"' where prefecture_ja='"+prefecture_ja+"'"
        cur.execute(qry)
        connection.commit()


def add_company_railwayline():
    qry = "alter table "+tb_to_mongodb_station + \
        " add column IF NOT EXISTS company_railwayline_ja text [], add column IF NOT EXISTS company_railwayline_en text [], add column IF NOT EXISTS company_railwayline_zh text []"
    cur.execute(qry)
    connection.commit()
    dict_company = {}
    qry = "select company_ja, company_en, company_zh from ctl_company"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        company_ja = each[0]
        company_en = each[1]
        company_zh = each[2]
        if "JR" in company_ja:
            company_ja = 'JR'
            company_en = 'JR'
            company_zh = 'JR'
        dict_company[company_ja] = [company_en, company_zh]
    dict_point = {}
    dict_railwayline = {}
    qry = "select company, railway_line_ja, railway_line_en, railway_line_zh, points from " + \
        tb_railway_line + " order by array_length(points,1) "
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        company = each[0]
        if 'JR' in company:
            company = 'JR'
        railway_line_ja = each[1]
        railway_line_en = each[2]
        railway_line_zh = each[3]

        if railway_line_ja not in dict_railwayline:
            dict_railwayline[railway_line_ja] = [
                railway_line_en, railway_line_zh]
        points = each[4]
        for p in points:
            if p not in dict_point:
                dict_point[p] = {}
            if company not in dict_point[p]:
                dict_point[p][company] = []
            if railway_line_ja not in dict_point[p][company]:
                dict_point[p][company].append(railway_line_ja)
    # Sort
    sort_dict_point = {}
    for p in dict_point:
        sort_dict_point[p] = {}
        list_company = dict_point[p].keys()
        list_line_count = []
        for c in list_company:
            list_line_count.append(len(dict_point[p][c]))
        sort_company_list = [x for y, x in sorted(
            zip(list_line_count, list_company))][::-1]

        new_list_company = []
        for c in sort_company_list:
            if 'JR' in c:
                new_list_company.append(c)
        for c in sort_company_list:
            if 'JR' not in c:
                new_list_company.append(c)
        for c in new_list_company:

            list_line = dict_point[p][c][::-1]
            sort_list_line = []

            if c == 'JR':
                for l in list_line:
                    if '新幹線' in l:
                        sort_list_line.append(l)
                for l in list_line:
                    if '新幹線' not in l:
                        sort_list_line.append(l)
            else:
                sort_list_line = list_line
            sort_dict_point[p][c] = sort_list_line
    for p in sort_dict_point:

        company_railwayline_ja = []
        company_railwayline_en = []
        company_railwayline_zh = []
        for c in sort_dict_point[p]:
            ja = c+"@"
            en = dict_company[c][0]+"@"
            zh = dict_company[c][1]+"@"
            for l in sort_dict_point[p][c]:
                ja = ja+l+","
                en = en+dict_railwayline[l][0]+","
                zh = zh+dict_railwayline[l][1]+","
            ja = ja[:-1]
            en = en[:-1]
            zh = zh[:-1]
            company_railwayline_ja.append(ja)
            company_railwayline_en.append(en)
            company_railwayline_zh.append(zh)
        qry = "update "+tb_to_mongodb_station+" set company_railwayline_ja=array"+str(company_railwayline_ja)+", company_railwayline_en=array"+str(
            company_railwayline_en)+", company_railwayline_zh=array"+str(company_railwayline_zh)+" where station_ja='"+p+"'"
        cur.execute(qry)
        connection.commit()


def add_company_serviceroute():
    qry = "alter table "+tb_to_mongodb_station + \
        " add column IF NOT EXISTS company_serviceroute_ja text [], add column IF NOT EXISTS company_serviceroute_en text [], add column IF NOT EXISTS company_serviceroute_zh text []"
    cur.execute(qry)
    connection.commit()
    dict_company = {}
    qry = "select company_ja, company_en, company_zh from ctl_company"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        company_ja = each[0]
        company_en = each[1]
        company_zh = each[2]
        if "JR" in company_ja:
            company_ja = 'JR'
            company_en = 'JR'
            company_zh = 'JR'
        dict_company[company_ja] = [company_en, company_zh]
    dict_point = {}
    dict_railwayline = {}
    qry = "select company, service_route_ja, service_route_en, service_route_zh, stations from " + \
        tb_service_route + " order by array_length(stations,1) "
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        company = each[0]
        if 'JR' in company:
            company = 'JR'
        railway_line_ja = each[1]
        railway_line_en = each[2]
        railway_line_zh = each[3]

        if railway_line_ja not in dict_railwayline:
            dict_railwayline[railway_line_ja] = [
                railway_line_en, railway_line_zh]
        points = each[4]
        for p in points:
            if p not in dict_point:
                dict_point[p] = {}
            if company not in dict_point[p]:
                dict_point[p][company] = []
            if railway_line_ja not in dict_point[p][company]:
                dict_point[p][company].append(railway_line_ja)
    # Sort
    sort_dict_point = {}
    for p in dict_point:
        sort_dict_point[p] = {}
        list_company = dict_point[p].keys()
        list_line_count = []
        for c in list_company:
            list_line_count.append(len(dict_point[p][c]))
        sort_company_list = [x for y, x in sorted(
            zip(list_line_count, list_company))][::-1]

        new_list_company = []
        for c in sort_company_list:
            if 'JR' in c:
                new_list_company.append(c)
        for c in sort_company_list:
            if 'JR' not in c:
                new_list_company.append(c)
        for c in new_list_company:

            list_line = dict_point[p][c][::-1]
            sort_list_line = []

            if c == 'JR':
                for l in list_line:
                    if '新幹線' in l:
                        sort_list_line.append(l)
                for l in list_line:
                    if '新幹線' not in l:
                        sort_list_line.append(l)
            else:
                sort_list_line = list_line
            sort_dict_point[p][c] = sort_list_line
    for p in sort_dict_point:
        company_railwayline_ja = []
        company_railwayline_en = []
        company_railwayline_zh = []
        for c in sort_dict_point[p]:
            ja = c+"@"
            en = dict_company[c][0]+"@"
            zh = dict_company[c][1]+"@"
            for l in sort_dict_point[p][c]:
                ja = ja+l+","
                en = en+dict_railwayline[l][0]+","
                zh = zh+dict_railwayline[l][1]+","
            ja = ja[:-1]
            en = en[:-1]
            zh = zh[:-1]
            company_railwayline_ja.append(ja)
            company_railwayline_en.append(en)
            company_railwayline_zh.append(zh)
        qry = "update "+tb_to_mongodb_station+" set company_serviceroute_ja=array"+str(company_railwayline_ja)+", company_serviceroute_en=array"+str(
            company_railwayline_en)+", company_serviceroute_zh=array"+str(company_railwayline_zh)+" where station_ja='"+p+"'"
        cur.execute(qry)
        connection.commit()


def upload_to_mongo():
    # 如果不存在这张表，后面将直接新建，如果已存在，此处会将期document全部删除
    mydb[new_station].delete_many({})
    qry = "SELECT station_ja, station_en, station_zh, prefecture_ja, prefecture_en, prefecture_zh, company_railwayline_ja, company_railwayline_en, company_railwayline_zh, company_serviceroute_ja, company_serviceroute_en, company_serviceroute_zh, ST_AsGeoJSON(geog, 8)::jsonb as way  from (select * ,ST_Transform( way, 4326) as geog from " + \
        tb_to_mongodb_station+") as a"
    cur.execute(qry)
    results = cur.fetchall()
    addStationList = []
    for each in results:
        addStationList.append({'station_ja': each[0], 'station_en': each[1], 'station_zh': each[2], 'prefecture_ja': each[3], 'prefecture_en': each[4], 'prefecture_zh': each[5], 'company_railwayline_ja': each[6],
                              'company_railwayline_en': each[7], 'company_railwayline_zh': each[8], 'company_serviceroute_ja': each[9], 'company_serviceroute_en': each[10], 'company_serviceroute_zh': each[11], 'way': each[12]})

    mydb[new_station].insert_many(addStationList)


create_new_to_mongodb_station_table()
complete_prefecture()
add_company_railwayline()
add_company_serviceroute()
upload_to_mongo()
