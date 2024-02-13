
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
new_serviceroute = 'serviceroute_'+config['version_code']

tb_polygon = 'polygon'
tb_line = 'line'
tb_railway_line = 'railway_line'
tb_service_route = 'service_route'
tb_to_mongodb_service_route = 'to_mongodb_service_route_' + \
    config['version_code']

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_to_mongodb_service_route_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_mongodb_service_route
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_mongodb_service_route + \
        " as select  service_route_ja,  service_route_en,  service_route_zh,  stations as stations_ja,company as company_ja, type, ra_keys from "+tb_service_route
    cur.execute(qry)
    connection.commit()


def complete_company_stations_prefecture():
    qry = "alter table "+tb_to_mongodb_service_route + \
        " add column company_en text, add column company_zh text, add column stations_en text[], add column stations_zh text[], add column prefecture_ja text[], add column prefecture_en text[], add column prefecture_zh text[]"
    cur.execute(qry)
    connection.commit()

    qry = 'select company_ja, company_en, company_zh from ctl_company'
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        company_ja = each[0]
        company_en = each[1]
        company_zh = each[2]
        qry = "update "+tb_to_mongodb_service_route+" set company_en='"+company_en + \
            "', company_zh='"+company_zh+"' where company_ja='"+company_ja+"'"
        cur.execute(qry)
        connection.commit()

    dict_prefecture = {}
    qry = "select prefecture_ja, prefecture_en, prefecture_zh from ctl_prefecture"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        prefecture_ja = each[0]
        prefecture_en = each[1]
        prefecture_zh = each[2]
        dict_prefecture[prefecture_ja] = [prefecture_en, prefecture_zh]

    dict_point = {}
    qry = "select name_ja, name_en, name_zh, prefecture from "+tb_polygon
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        name_ja = each[0]
        name_en = each[1]
        name_zh = each[2]
        prefecture_ja = each[3]
        prefecture_en = dict_prefecture[prefecture_ja][0]
        prefecture_zh = dict_prefecture[prefecture_ja][1]

        dict_point[name_ja] = [name_en, name_zh,
                               prefecture_ja, prefecture_en, prefecture_zh]

    qry = "select stations_ja from "+tb_to_mongodb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        stations_ja = each[0]
        stations_en = []
        stations_zh = []
        prefecture_ja = []
        prefecture_en = []
        prefecture_zh = []
        for p in stations_ja:
            stations_en.append(dict_point[p][0])
            stations_zh.append(dict_point[p][1])
            prefecture_ja.append(dict_point[p][2])
            prefecture_en.append(dict_point[p][3])
            prefecture_zh.append(dict_point[p][4])
        qry = "update "+tb_to_mongodb_service_route+" set stations_en=array"+str(stations_en)+", stations_zh=array"+str(stations_zh)+", prefecture_ja=array"+str(
            prefecture_ja)+", prefecture_en=array"+str(prefecture_en)+", prefecture_zh=array"+str(prefecture_zh)+" where stations_ja=array"+str(stations_ja)

        cur.execute(qry)
        connection.commit()


def add_acc_distance():
    qry = "ALTER TABLE "+tb_to_mongodb_service_route + \
        " ADD COLUMN IF NOT EXISTS acc_distance numeric[]"
    cur.execute(qry)
    connection.commit()
    dict_ra_key = {}
    qry = "select ra_key,distance from "+tb_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_key = each[0]
        distance = each[1]
        dict_ra_key[ra_key] = distance
    qry = "select stations, ra_keys, points from "+tb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        station_list = each[0]
        ra_keys = each[1]
        points = each[2]
        ra_key_distance = list(map(lambda x: dict_ra_key[x], ra_keys))
        list_acc_distance = []
        idx = 0
        for station in station_list:
            idx = points.index(station, idx)
            if idx > 0:
                acc_distance = str(round(sum(ra_key_distance[:idx]), 1))
                list_acc_distance.append(acc_distance)
            else:
                list_acc_distance.append('0')

        qry = "update "+tb_to_mongodb_service_route+" set acc_distance=array"+str(list_acc_distance).replace(
            "'", "")+" where ra_keys=array"+str(ra_keys)+" and stations_ja=array"+str(station_list)

        cur.execute(qry)
        connection.commit()


def add_including_service_route():
    qry = "ALTER TABLE "+tb_to_mongodb_service_route + \
        " ADD COLUMN IF NOT EXISTS including_service_route_ja text[], ADD COLUMN IF NOT EXISTS including_service_route_en text[],ADD COLUMN IF NOT EXISTS including_service_route_zh text[]"
    cur.execute(qry)
    connection.commit()
    dict_station = {}
    dict_service_route = {}
    qry = "select stations, service_route_ja, service_route_en, service_route_zh from " + \
        tb_service_route + " order by array_length (stations, 1) desc"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        stations = each[0]
        service_route_ja = each[1]
        service_route_en = each[2]
        service_route_zh = each[3]
        for station in stations:
            if station not in dict_station:
                dict_station[station] = []
            if service_route_ja not in dict_station[station]:
                dict_station[station].append(service_route_ja)
        if service_route_ja not in dict_service_route:
            dict_service_route[service_route_ja] = [
                service_route_en, service_route_zh]

    qry = "select stations_ja from "+tb_to_mongodb_service_route
    cur.execute(qry)
    r = cur.fetchall()

    for each in r:
        stations_ja = each[0]
        list_service_route_ja = []
        list_service_route_en = []
        list_service_route_zh = []
        for point in stations_ja:
            if point in dict_station:
                service_route_ja = dict_station[point]
                sorted_service_route_ja = []
                sorted_service_route_en = []
                sorted_service_route_zh = []
                for line in service_route_ja:
                    if '新幹線' in line:
                        sorted_service_route_ja.append(line)
                for line in service_route_ja:
                    if '新幹線' not in line and 'JR' in line:
                        sorted_service_route_ja.append(line)
                for line in service_route_ja:
                    if 'JR' not in line and line not in sorted_service_route_ja:
                        sorted_service_route_ja.append(line)
                        left_2_word = line[:2]
                        for l in service_route_ja:
                            if 'JR' not in l and l not in sorted_service_route_ja and l[:2] == left_2_word:
                                sorted_service_route_ja.append(l)

                for line in sorted_service_route_ja:
                    sorted_service_route_en.append(dict_service_route[line][0])
                    sorted_service_route_zh.append(dict_service_route[line][1])
                no_bracket_sorted_service_route_ja = []
                no_bracket_sorted_service_route_en = []
                no_bracket_sorted_service_route_zh = []
                for route in sorted_service_route_ja:
                    no_bracket_route = route.split("(")[0]
                    if no_bracket_route not in no_bracket_sorted_service_route_ja:
                        no_bracket_sorted_service_route_ja.append(
                            no_bracket_route)
                for route in sorted_service_route_en:
                    no_bracket_route = route.split("(")[0]
                    if no_bracket_route not in no_bracket_sorted_service_route_en:
                        no_bracket_sorted_service_route_en.append(
                            no_bracket_route)

                for route in sorted_service_route_zh:
                    no_bracket_route = route.split("(")[0]
                    if no_bracket_route not in no_bracket_sorted_service_route_zh:
                        no_bracket_sorted_service_route_zh.append(
                            no_bracket_route)

                str_service_route_ja = str(no_bracket_sorted_service_route_ja).replace(
                    '[', '').replace(']', '').replace("'", "").replace(" ", "")
                str_service_route_en = str(no_bracket_sorted_service_route_en).replace(
                    '[', '').replace(']', '').replace("'", "").split("(")[0]
                str_service_route_zh = str(no_bracket_sorted_service_route_zh).replace(
                    '[', '').replace(']', '').replace("'", "").replace(" ", "")
                list_service_route_ja.append(str_service_route_ja)
                list_service_route_en.append(str_service_route_en)
                list_service_route_zh.append(str_service_route_zh)
            else:
                list_service_route_ja.append('')
                list_service_route_en.append('')
                list_service_route_zh.append('')
        qry = "update "+tb_to_mongodb_service_route+" set including_service_route_ja=array"+str(list_service_route_ja)+", including_service_route_en=array"+str(
            list_service_route_en)+", including_service_route_zh=array"+str(list_service_route_zh)+" where stations_ja=array"+str(stations_ja)
        cur.execute(qry)
        connection.commit()


def add_way_bounds():
    qry = "ALTER TABLE "+tb_to_mongodb_service_route + \
        " ADD COLUMN IF NOT EXISTS way_bounds geometry"
    cur.execute(qry)
    connection.commit()
    qry = "select ra_keys from "+tb_to_mongodb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_keys = each[0]
        qry = "update "+tb_to_mongodb_service_route+" set way_bounds=(select ST_Envelope(ST_LineMerge(ST_Union(c.way))) from (select way from "+tb_line + \
            " a where a.ra_key =ANY ((select ra_keys from "+tb_to_mongodb_service_route + \
            " b where b.ra_keys = array" + \
            str(ra_keys)+")::text[]) ) as c) where ra_keys=array"+str(ra_keys)
        cur.execute(qry)
        connection.commit()


def add_including_railway_line():
    dict_line = {}
    statement = "select ra_key, railway_line from " + \
        tb_line + " where railway_line is not null"
    cur.execute(statement)
    results = cur.fetchall()
    for each in results:
        ra_key = each[0]
        gis_line = each[1]
        dict_line[ra_key] = gis_line

    dict_translate_railway_line = {}
    qry = "select railway_line_ja, ra_keys,railway_line_en, railway_line_zh from "+tb_railway_line
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        gis_line = each[0]
        ra_keys = each[1]
        railway_line_en = each[2]
        railway_line_zh = each[3]
        if gis_line not in dict_translate_railway_line:
            dict_translate_railway_line[gis_line] = [
                railway_line_en, railway_line_zh]
        for ra_key in ra_keys:
            if ra_key not in dict_line:
                dict_line[ra_key] = gis_line
        if '(' in gis_line:  # 特别的连接线，找不到对应的railway_line，如东海道本线，在railway_line里都是带括号的
            gis_line = each[0].split('(')[0]
            railway_line_en = each[2].split('(')[0]
            railway_line_zh = each[3].split('(')[0]
            if gis_line not in dict_translate_railway_line:
                dict_translate_railway_line[gis_line] = [
                    railway_line_en, railway_line_zh]

    qry = "ALTER TABLE "+tb_to_mongodb_service_route + \
        " ADD COLUMN IF NOT EXISTS including_railway_line_ja text[], ADD COLUMN IF NOT EXISTS including_railway_line_en text[],ADD COLUMN IF NOT EXISTS including_railway_line_zh text[]"
    cur.execute(qry)
    connection.commit()

    statement = "Select  service_route_ja, stations, ra_keys, points from "+tb_service_route
    cur.execute(statement)
    results = cur.fetchall()
    for each in results:
        service_route = each[0]
        station_list = each[1]
        ra_keys = each[2]
        list_ra_key_gis_line = list(map(lambda x: dict_line[x], ra_keys))
        points = each[3]
        list_gis_line = []
        list_gis_line_en = []
        list_gis_line_zh = []
        pre_idx = 0
        for station in station_list:
            idx = points.index(station, pre_idx)
            if idx > 0:
                this_list = []
                this_list_en = []
                this_list_zh = []
                ra_key_gis_line = list_ra_key_gis_line[pre_idx:idx]
                for gis_line in ra_key_gis_line:
                    if gis_line not in this_list:
                        this_list.append(gis_line)
                        this_list_en.append(
                            dict_translate_railway_line[gis_line][0])
                        this_list_zh.append(
                            dict_translate_railway_line[gis_line][1])
                list_gis_line.append(str(this_list).replace(
                    '[', '').replace(']', '').replace("'", ''))
                list_gis_line_en.append(str(this_list_en).replace(
                    '[', '').replace(']', '').replace("'", ''))
                list_gis_line_zh.append(str(this_list_zh).replace(
                    '[', '').replace(']', '').replace("'", ''))
            else:
                list_gis_line.append('')
                list_gis_line_en.append('')
                list_gis_line_zh.append('')
            pre_idx = idx

        statement = "update "+tb_to_mongodb_service_route+" set including_railway_line_ja=array"+str(list_gis_line)+", including_railway_line_en=array"+str(
            list_gis_line_en)+", including_railway_line_zh=array"+str(list_gis_line_zh)+" where service_route_ja='"+service_route+"' and ra_keys=array"+str(ra_keys)
        cur.execute(statement)
        connection.commit()


def add_priority():
    # 与g03_to_mongodb_railwayline里的add_priority的逻辑基本一致,差异在于一个ra_key可能对应多个service_route
    qry = "ALTER TABLE "+tb_to_mongodb_service_route + \
        " ADD COLUMN IF NOT EXISTS priority integer"
    cur.execute(qry)
    connection.commit()

    qry = "UPDATE "+tb_to_mongodb_service_route + \
        " set priority=0"
    cur.execute(qry)
    connection.commit()

    # 1.建立dict_ra_keys,  从service_route中获取信息(dict_ra_keys[jtss_009]=['JR上越新幹線','JR东北新干线'])
    # 2.建立dict_stations, 从ctl_stations里取到ra_keys,连接第一步，拼成dict_stations[str(statsions)]=['上越新干线','东北新干线']
    # 3.建立dict_railway_line(dict_railway_line[railway_line]=5),从train里取到stations，每一个stations,在第2步的字典里找到线路，在dict_railway_line里+1

    # 1.
    qry = "select ra_keys, service_route_ja from service_route"
    cur.execute(qry)
    r = cur.fetchall()
    dict_ra_keys = {}
    for each in r:
        ra_keys = each[0]
        service_route_ja = each[1]
        for key in ra_keys:
            if key not in dict_ra_keys:
                dict_ra_keys[key] = []
            if service_route_ja not in dict_ra_keys:
                dict_ra_keys[key].append(service_route_ja)

    # 2.
    qry = "select stations, ra_keys from ctl_stations"
    cur.execute(qry)
    r = cur.fetchall()
    dict_stations = {}
    for each in r:
        stations = each[0]
        ra_keys = each[1]
        dict_stations[str(stations)] = []
        for key in ra_keys:
            if key in dict_ra_keys:
                lines = dict_ra_keys[key]
                for line in lines:
                    if line not in dict_stations[str(stations)]:
                        dict_stations[str(stations)].append(line)

    # 3.
    qry = "select distinct(stations) from train "
    cur.execute(qry)
    r = cur.fetchall()
    dict_service_route = {}
    for each in r:
        stations = each[0]
        lines = dict_stations[str(stations)]
        for line in lines:
            if line not in dict_service_route:
                dict_service_route[line] = 0
            dict_service_route[line] += 1
    for line in dict_service_route:
        qry = "update "+tb_to_mongodb_service_route+" set priority=" + \
            str(dict_service_route[line]) + \
            " where service_route_ja='"+line+"'"
        cur.execute(qry)
        connection.commit()


def upload_to_mongo():
    # 如果不存在这张表，后面将直接新建，如果已存在，此处会将期document全部删除
    mydb[new_serviceroute].delete_many({})
    qry = "SELECT service_route_ja, service_route_en, service_route_zh, company_ja, company_en,  company_zh,  type, ra_keys, stations_ja, stations_en, stations_zh, prefecture_ja,  prefecture_en, prefecture_zh,  priority, including_railway_line_ja, including_railway_line_en,  including_railway_line_zh, including_service_route_ja, including_service_route_en,  including_service_route_zh,  acc_distance,  ST_AsGeoJSON(geog, 5)::jsonb as way_bounds from (select * ,ST_Transform( way_BOUNDS, 4326) as geog from " + \
        tb_to_mongodb_service_route+") as a"
    cur.execute(qry)
    results = cur.fetchall()
    addServicerouteList = []
    for each in results:
        acc_distance = each[21]
        new_acc_distance = []
        for d in acc_distance:
            new_acc_distance.append(float(d))

        addServicerouteList.append({'serviceroute_ja': each[0], 'serviceroute_en': each[1], 'serviceroute_zh': each[2], 'company_ja': each[3], 'company_en': each[4], 'company_zh': each[5], 'type': each[6], 'ra_keys': each[7], 'stations_ja': each[8], 'stations_en': each[9], 'stations_zh': each[10], 'prefecture_ja': each[11], 'prefecture_en': each[12],
                                   'prefecture_zh': each[13], 'priority': each[14], 'including_railway_line_ja': each[15], 'including_railway_line_en': each[16], 'including_railway_line_zh': each[17], 'including_service_route_ja': each[18], 'including_service_route_en': each[19], 'including_service_route_zh': each[20], 'acc_distance': new_acc_distance,  'way_bounds': each[22]})
    mydb[new_serviceroute].insert_many(addServicerouteList)


create_new_to_mongodb_service_route_table()
complete_company_stations_prefecture()
add_acc_distance()
add_including_service_route()
add_way_bounds()
add_including_railway_line()
add_priority()
upload_to_mongo()
