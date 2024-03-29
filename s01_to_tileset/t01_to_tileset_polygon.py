import configparser
import psycopg2
from configparser import ConfigParser
config_parser = ConfigParser()
config_parser.read('../config.cfg')
config = config_parser['DEFAULT']
database = config['database']+'_'+config['version_code']
hostname = config['hostname']
username = config['username']
password = config['password']
tb_to_tileset_polygon = 'to_tileset_polygon_'+config['version_code']
tb_polygon = 'polygon'
tb_railway_line = 'railway_line'
tb_service_route = 'service_route'
tb_line = 'line'

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_polygon_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_tileset_polygon
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_tileset_polygon + \
        " as select st_buffer(way, 20) as way, name_ja as name, SPLIT_PART(name_ja, '(',1)  as name_ja, SPLIT_PART(name_en, '(',1)  as name_en,  SPLIT_PART(name_zh, '(',1)  as name_zh , prefecture as prefecture_ja from "+tb_polygon
    cur.execute(qry)
    connection.commit()


def update_prefecture():
    # ADD prefecture_en, prefecture_cn
    qry = "alter table "+tb_to_tileset_polygon + \
        " add column prefecture_en text, add column prefecture_zh text"
    cur.execute(qry)
    connection.commit()
    qry = "select prefecture_ja, prefecture_en, prefecture_zh from ctl_prefecture"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        prefecture = each[0]
        prefecture_en = each[1]
        prefecture_cn = each[2]
        qry = "update "+tb_to_tileset_polygon+" set prefecture_en='"+prefecture_en + \
            "', prefecture_zh='"+prefecture_cn+"' where prefecture_ja='"+prefecture+"'"
        cur.execute(qry)
        connection.commit()

# Python code to sort the tuples using second element
# of sublist Inplace way to sort using sort()


def sort_second(sub_li):
    sub_li.sort(key=lambda x: x[1])
    reverse_li = sub_li.reverse()
    return reverse_li


def add_railway_line():
    qry = "alter table "+tb_to_tileset_polygon + \
        " add column railway_line_ja text[], add column railway_line_en text[], add column railway_line_zh text[]"
    cur.execute(qry)
    connection.commit()

    dict_railway_line = {}  # dict_railway_line[point]=[line_1, line_2]
    dict_railway_type = {}  # dict_railway_type[line]='type'
    dict_railway_points_count = {}  # dict_railway_points_count[line]=21
    qry = "select railway_line_ja, points, type, company from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        railway_line = each[0].split('(')[0]
        points = each[1]
        the_type = each[2]
        company = each[3].split('(')[0]
        if company[:2] == 'JR':
            company = 'JR'
        railway_line = railway_line+"@"+company
        for p in points:
            if p not in dict_railway_line:
                dict_railway_line[p] = []
            if railway_line not in dict_railway_line[p]:
                dict_railway_line[p].append(railway_line)
        if railway_line not in dict_railway_type:
            dict_railway_type[railway_line] = the_type
            dict_railway_points_count[railway_line] = 0
        dict_railway_points_count[railway_line] += len(points)

    for point in dict_railway_line:
        # 排序
        lines = dict_railway_line[point]
        ordered_lines = []
        shinkansen_lines = []
        rail_jr_lines = []
        rail_private_lines = []
        other_lines = []

        for l in lines:
            if dict_railway_type[l] == 'shinkansen':
                shinkansen_lines.append([l, dict_railway_points_count[l]])
            elif dict_railway_type[l] == 'rail_JR':
                rail_jr_lines.append([l, dict_railway_points_count[l]])
            elif dict_railway_type[l] == 'rail_private':
                rail_private_lines.append([l, dict_railway_points_count[l]])
            else:
                other_lines.append([l, dict_railway_points_count[l]])
        sort_second(shinkansen_lines)
        sort_second(rail_jr_lines)
        sort_second(rail_private_lines)
        sort_second(other_lines)
        for l in shinkansen_lines:
            ordered_lines.append(l[0])
        for l in rail_jr_lines:
            ordered_lines.append(l[0])

        while len(rail_private_lines) > 0:
            the_company = rail_private_lines[0][0].split("@", 1)
            for l in list(rail_private_lines):
                company = l[0].split('@', 1)
                if company == the_company:
                    ordered_lines.append(l[0])
                    rail_private_lines.remove(l)

        while len(other_lines) > 0:
            the_company = other_lines[0][0].split("@", 1)
            for l in list(other_lines):
                company = l[0].split('@', 1)
                if company == the_company:
                    ordered_lines.append(l[0])
                    other_lines.remove(l)
        qry = "update "+tb_to_tileset_polygon+" set railway_line_ja=array" + \
            str(ordered_lines)+" where name='"+point+"'"
        cur.execute(qry)
        connection.commit()


def add_en_cn_for_railway_line():
    dict_company = {}  # dict_company['JR']=['JR','JR']
    qry = "select company_ja, company_en, company_zh from ctl_company"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        company = each[0].split('(')[0]
        company_en = each[1].split('(')[0]
        company_cn = each[2].split('(')[0]
        if company[:2] != 'JR':
            if company not in dict_company:
                dict_company[company] = []
            dict_company[company] = [company_en, company_cn]
    dict_company['JR'] = ['JR', 'JR']

    dict_line = {}  # dict['東北新幹線 JR']=['dongbei shinkansen @JR', '东北新干线@JR']
    qry = "select railway_line_ja, railway_line_en, railway_line_zh, company from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        railway_line = each[0].split('(')[0]
        railway_line_en = each[1].split("(")[0]
        railway_line_cn = each[2].split('(')[0]
        company = each[3].split("(")[0]
        if company[:2] == 'JR':
            company = 'JR'
        company_en = dict_company[company][0]
        company_cn = dict_company[company][1]
        line = railway_line+"@"+company
        line_en = railway_line_en+"@"+company_en
        line_cn = railway_line_cn+"@"+company_cn
        if line not in dict_line:
            dict_line[line] = [line_en, line_cn]
    qry = "select name, railway_line_ja from " + \
        tb_to_tileset_polygon+" where railway_line_ja is not null"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        name = each[0]
        railway_lines = each[1]
        railway_line_ens = []
        railway_line_cns = []
        for l in railway_lines:
            railway_line_ens.append(dict_line[l][0])
            railway_line_cns.append(dict_line[l][1])
        qry = "update "+tb_to_tileset_polygon+" set railway_line_en=array" + \
            str(railway_line_ens)+", railway_line_zh=array" + \
            str(railway_line_cns)+" where name='"+name+"'"
        cur.execute(qry)
        connection.commit()


def add_service_route():
    qry = "alter table "+tb_to_tileset_polygon + \
        " add column service_route_ja text[], add column service_route_en text[], add column service_route_zh text[]"
    cur.execute(qry)
    connection.commit()

    dict_service_route = {}  # dict_railway_line[point]=[line_1, line_2]
    dict_service_type = {}  # dict_railway_type[line]='type'
    dict_service_points_count = {}  # dict_railway_points_count[line]=21
    qry = "select service_route_ja, stations, type, company from "+tb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        service_route = each[0].split('(')[0]
        stations = each[1]
        the_type = each[2]
        company = each[3].split('(')[0]
        if company[:2] == 'JR':
            company = 'JR'
        service_route = service_route+"@"+company
        for s in stations:
            if s not in dict_service_route:
                dict_service_route[s] = []
            if service_route not in dict_service_route[s]:
                dict_service_route[s].append(service_route)
        if service_route not in dict_service_type:
            dict_service_type[service_route] = the_type
            dict_service_points_count[service_route] = 0
        dict_service_points_count[service_route] += len(stations)

    for station in dict_service_route:
        # 排序
        lines = dict_service_route[station]
        ordered_lines = []
        shinkansen_lines = []
        rail_jr_lines = []
        rail_private_lines = []
        other_lines = []

        for l in lines:
            if dict_service_type[l] == 'shinkansen':
                shinkansen_lines.append([l, dict_service_points_count[l]])
            elif dict_service_type[l] == 'rail_JR':
                rail_jr_lines.append([l, dict_service_points_count[l]])
            elif dict_service_type[l] == 'rail_private':
                rail_private_lines.append([l, dict_service_points_count[l]])
            else:
                other_lines.append([l, dict_service_points_count[l]])
        sort_second(shinkansen_lines)
        sort_second(rail_jr_lines)
        sort_second(rail_private_lines)
        sort_second(other_lines)
        for l in shinkansen_lines:
            ordered_lines.append(l[0])
        for l in rail_jr_lines:
            ordered_lines.append(l[0])

        while len(rail_private_lines) > 0:
            the_company = rail_private_lines[0][0].split("@", 1)
            for l in list(rail_private_lines):
                company = l[0].split('@', 1)
                if company == the_company:
                    ordered_lines.append(l[0])
                    rail_private_lines.remove(l)

        while len(other_lines) > 0:
            the_company = other_lines[0][0].split("@", 1)
            for l in list(other_lines):
                company = l[0].split('@', 1)
                if company == the_company:
                    ordered_lines.append(l[0])
                    other_lines.remove(l)
        qry = "update "+tb_to_tileset_polygon+" set service_route_ja=array" + \
            str(ordered_lines)+" where SPLIT_PART(name, '[',1)='"+station+"'"
        cur.execute(qry)
        connection.commit()


def add_en_cn_for_service_route():

    # dict['東北新幹線 JR']=['dongbei shinkansen @JR', '东北新干线@JR']
    dict_service = {}
    qry = "select service_route_ja, service_route_en, service_route_zh from "+tb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        service_route = each[0].split('(')[0]
        service_route_en = each[1].split("(")[0]
        service_route_cn = each[2].split('(')[0]
        if service_route not in dict_service:
            dict_service[service_route] = [service_route_en, service_route_cn]
    qry = "select name, service_route_ja from " + \
        tb_to_tileset_polygon+" where service_route_ja is not null"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        name = each[0]
        service_routes = each[1]
        new_service_routes = []
        service_route_ens = []
        service_route_cns = []
        for r in service_routes:
            service_route = r.split('@')[0]
            new_service_routes.append(service_route)
            service_route_ens.append(dict_service[service_route][0])
            service_route_cns.append(dict_service[service_route][1])
        qry = "update "+tb_to_tileset_polygon+" set service_route_ja=array"+str(new_service_routes)+", service_route_en=array"+str(
            service_route_ens)+", service_route_zh=array"+str(service_route_cns)+" where name='"+name+"'"
        cur.execute(qry)
        connection.commit()


def add_type():
    qry = "alter table "+tb_to_tileset_polygon + " add column type text[]"
    cur.execute(qry)
    connection.commit()
    dict_point_type = {}  # dict[新大阪]=['shinkansen','rail_jr','subsay' ]

    qry = "select type, points from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        line_type = ''
        if 'shinkansen' == each[0]:
            line_type = 'shinkansen'
        elif 'rail_JR' == each[0]:
            line_type = 'rail_jr'
        elif 'rail_private' == each[0]:
            line_type = 'rail_private'
        elif 'subway' == each[0]:
            line_type = 'subway'
        elif 'tram' == each[0]:
            line_type = 'tram'
        else:
            line_type = 'others'
        points = each[1]
        for p in points:
            if p not in dict_point_type:
                dict_point_type[p] = []
            if line_type not in dict_point_type[p]:
                dict_point_type[p].append(line_type)
    for p in dict_point_type:
        qry = "update "+tb_to_tileset_polygon + \
            " set type=array"+str(dict_point_type[p])+" where name='"+p+"'"
        cur.execute(qry)
        connection.commit()
    qry = "select name from "+tb_to_tileset_polygon + " where type is null"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        point = each[0]
        qry = "select railway_line from " + tb_line + \
            " where start_point='"+point+"' or end_point='"+point+"'"
        cur.execute(qry)
        result = cur.fetchall()
        railwayline = result[0][0]
        qry = "select type from "+tb_railway_line + \
            " where railway_line_ja='"+railwayline+"'"
        cur.execute(qry)
        result = cur.fetchall()
        line_type = ''
        if 'shinkansen' == result[0][0]:
            line_type = 'shinkansen'
        elif 'rail_JR' == result[0][0]:
            line_type = 'rail_jr'
        elif 'rail_private' == result[0][0]:
            line_type = 'rail_private'
        elif 'subway' == result[0][0]:
            line_type = 'subway'
        elif 'tram' == result[0][0]:
            line_type = 'tram'
        else:
            line_type = 'others'
        qry = "update "+tb_to_tileset_polygon + \
            " set type=array"+str([line_type]) + \
            " where type is null and name='"+point+"'"
        cur.execute(qry)
        connection.commit()


def update_priority():

    qry = "ALTER TABLE "+tb_to_tileset_polygon + \
        " ADD COLUMN IF NOT EXISTS priority integer"
    cur.execute(qry)
    connection.commit()

    qry = "UPDATE "+tb_to_tileset_polygon + \
        " set priority=0"
    cur.execute(qry)
    connection.commit()
    # 根据train表里stations的次数来建立dict_stations的权重，结果在0到1263之间
    qry = "select distinct(stations) from train "
    cur.execute(qry)
    r = cur.fetchall()
    dict_stations = {}
    for each in r:
        stations = each[0]
        for station in stations:
            if station not in dict_stations:
                dict_stations[station] = 0
            dict_stations[station] += 1
        first_station = stations[0]
        last_station = stations[-1]
        dict_stations[first_station] += 1
        dict_stations[last_station] += 1
    # 根据线路数据建priority
    qry = "select service_route_ja, name, type from " + \
        tb_to_tileset_polygon + " where  service_route_ja is not null"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        service_route = each[0]
        name = each[1]
        type = each[2]
        priority = 0
        if name in dict_stations:
            priority = dict_stations[name]
        if 'shinkansen' in type:
            priority += 150
        if 'rail_private' in type:
            priority += 100
        if 'rail_jr' in type:
            priority += 100
        priority += len(service_route)*30
        qry = "update "+tb_to_tileset_polygon + " set priority=" + \
            str(priority)+" where name='"+name+"'"
        cur.execute(qry)
        connection.commit()

    qry = "update "+tb_to_tileset_polygon + " set priority=0 where priority is null"
    cur.execute(qry)
    connection.commit()

    qry = "update "+tb_to_tileset_polygon + \
        " set priority=100000-priority"  # 这个大小顺序目前不明确，暂按数值最小的权重最大来
    cur.execute(qry)
    connection.commit()


create_new_polygon_table()
update_prefecture()
add_railway_line()
add_en_cn_for_railway_line()
add_service_route()
add_en_cn_for_service_route()
add_type()
update_priority()
