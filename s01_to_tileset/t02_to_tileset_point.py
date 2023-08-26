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
tb_to_tileset_point = 'to_tileset_point_'+config['version_code']
tb_to_tileset_polygon = 'to_tileset_polygon_'+config['version_code']
tb_line = 'line'
tb_service_route = 'service_route'


connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()

# 一、从tb_to_tileset_polygon copy主要表的字段，以POINT不GIS字段，其后删除表中的所有信息，这一步仅建立表

# 二、增加三个字段，gis_railway_line text[], gis_service_route text[], gis_type text (另外需加三个暂助字段railway_line_temp text，ra_key_temp text, ra_keys_temp text[], 在第三步中需要用到，在第四步中删除）


# 三、找出所有line的首尾点的GIS点，维护好gis_railway_line，gis_service_route字段，他们有两种:
# 1. 同一名字的station只有一个GIS点, gis_type='gis_unique', (在所有图中均出现)
# 2. 同一名字的station有多个GIS点, gis_type='gis_in_line' （只在railway_line和service_route中出现）
# 针对第二种，再增加一类点：
# 3. 第二种同一名字的station,找到多个GIS点的st_centroid(way), show_in_map='gis_centroid' （在除了railway_line和service_route的页面中出现),这类点没有gis_railway_line text[], gis_service_route text[]

# 四、除以上三个字段外，其他信息均以其名字copyPolygon,删除railway_line_temp,service_route_temp字段


def create_new_point_table():
    # 第一、二步
    qry = "DROP TABLE IF EXISTS "+tb_to_tileset_point
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_tileset_point + \
        " as select st_centroid(way) as way, priority, name, name_ja, name_en, name_zh, prefecture_ja,  prefecture_en, prefecture_zh, railway_line_ja, railway_line_en, railway_line_zh, service_route_ja, service_route_en, service_route_zh, type from "+tb_to_tileset_polygon + " limit 1"
    cur.execute(qry)
    connection.commit()

    qry = "DELETE FROM "+tb_to_tileset_point
    cur.execute(qry)
    connection.commit()

    qry = "ALTER TABLE "+tb_to_tileset_point + \
        " ADD COLUMN gis_railway_line text[], ADD COLUMN gis_service_route text[], ADD COLUMN gis_type text, ADD COLUMN railway_line_temp text, ADD COLUMN ra_key_temp text, ADD COLUMN ra_keys_temp text[]"
    cur.execute(qry)
    connection.commit()


def add_gis_point():
    # 1. 从line入手，所有line的首尾点全部加进来
    qry = "INSERT INTO "+tb_to_tileset_point + \
        " (way, name, railway_line_temp, ra_key_temp) select st_startpoint(way), start_point, railway_line, ra_key from "+tb_line
    cur.execute(qry)
    connection.commit()

    qry = "INSERT INTO "+tb_to_tileset_point + \
        " (way, name, railway_line_temp, ra_key_temp) select st_endpoint(way), end_point, railway_line, ra_key from "+tb_line
    cur.execute(qry)
    connection.commit()


def update_gis_railway_line():
    # 对于gis_railway_line is null的点，按name, railway_line_temp, ra_key_temp选出一个来,再找出与它intersection的点，更新gis_railway_line, ra_keys_temp

    isDone = False
    while not isDone:
        qry = 'select name, railway_line_temp, ra_key_temp from ' + \
            tb_to_tileset_point + \
            " where gis_railway_line is null limit 1"
        cur.execute(qry)
        r = cur.fetchall()
        if len(r) == 0:
            isDone = True
        else:
            for each in r:
                name = each[0]
                railway_line_temp = each[1]
                ra_key = each[2]
                # 看有没有点如其相交
                qry = "select b.railway_line_temp, b.ra_key_temp from "+tb_to_tileset_point+" a, "+tb_to_tileset_point+" b where b.name='" + \
                    name+"' and b.ra_key_temp !='"+ra_key+"' and a.name ='"+name + \
                    "' and a.ra_key_temp='"+ra_key + \
                    "' and st_intersects(a.way, b.way)"
                print(qry)
                cur.execute(qry)
                r = cur.fetchall()
                # 无论是否相交，均可直接更新该点的gis_railway_line, ra_keys_temp
                gis_rail_line = [railway_line_temp]
                ra_keys_temp = [ra_key]

                if len(r) > 0:
                    for each in r:
                        if each[0] not in gis_rail_line:
                            gis_rail_line.append(each[0])
                        if each[1] not in ra_keys_temp:
                            ra_keys_temp.append(each[1])
                qry = "update "+tb_to_tileset_point+" set gis_railway_line =array" + \
                    str(gis_rail_line)+", ra_keys_temp=array"+str(ra_keys_temp)+" where name='"+name + \
                    "' and ra_key_temp='"+ra_key+"'"
                print(qry)
                cur.execute(qry)
                connection.commit()
                # 更新了gis_railway_line以后，即可删除相交的点了
                qry = "DELETE from "+tb_to_tileset_point+" b USING "+tb_to_tileset_point+" a where b.name='" + \
                    name+"' and b.ra_key_temp !='"+ra_key+"' and a.name ='"+name + \
                    "' and a.ra_key_temp='"+ra_key + \
                    "' and st_intersects(a.way, b.way)"
                print(qry)
                cur.execute(qry)
                connection.commit()


def update_gis_service_route():
    #  以上的每一个点，根据name和ra_keys_temp，更新gis_service_route
    dict_point = {}
    dict_ra_key = {}
    qry = "select service_route_ja, ra_keys, stations from "+tb_service_route
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        service_route = each[0]
        ra_keys = each[1]
        stations = each[2]
        for k in ra_keys:
            if k not in dict_ra_key:
                dict_ra_key[k] = []
            if service_route not in dict_ra_key[k]:
                dict_ra_key[k].append(service_route)
        for s in stations:
            if s not in dict_point:
                dict_point[s] = []
            if service_route not in dict_point[s]:
                dict_point[s].append(service_route)
    qry = "select name, ra_keys_temp from "+tb_to_tileset_point
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        name = each[0]
        ra_keys = each[1]
        gis_service_route = []
        if name in dict_point:
            possible_service_routes = dict_point[name]
            for route in possible_service_routes:
                if route not in gis_service_route:
                    for k in ra_keys:
                        if k in dict_ra_key:
                            if route in dict_ra_key[k]:
                                if route not in gis_service_route:
                                    gis_service_route.append(route)
        if len(gis_service_route) == 0:
            gis_service_route = ['']
        qry = "update "+tb_to_tileset_point+" set gis_service_route=array" + \
            str(gis_service_route)+" where name='"+name + \
            "' and ra_keys_temp=array"+str(ra_keys)
        print(qry)
        cur.execute(qry)
        connection.commit()


def update_gis_type():
    # 至此，如果name只有一个的点，gis_type='gis_unique',其他则为'gis_in_line'
    qry = "select name, count(name) from " + \
        tb_to_tileset_point+" group by name "
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        name = each[0]
        gis_type = ''
        if each[1] == 1:
            gis_type = 'gis_unique'
        else:
            gis_type = 'gis_in_line'
        qry = "update "+tb_to_tileset_point + \
            " set gis_type='"+gis_type+"' where name='"+name+"'"
        print(qry)
        cur.execute(qry)
        connection.commit()


def insert_centroid():
    qry = "select distinct(name) from "+tb_to_tileset_point + \
        " where gis_type='gis_in_line'"
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        name = each[0]
        qry = "insert into "+tb_to_tileset_point + \
            " (way, name, gis_type) values ((select st_centroid(st_union(way)) from " + \
            tb_to_tileset_point+" where name='"+name+"'), '"+name+"', 'gis_centroid')"
        print(qry)
        cur.execute(qry)
        connection.commit()


def update_everything():
    qry = "update "+tb_to_tileset_point + \
        " b set priority=a.priority, name_ja=a.name_ja, name_en=a.name_en, name_zh=a.name_zh, prefecture_ja=a.prefecture_ja, prefecture_en=a.prefecture_en, prefecture_zh=a.prefecture_zh, railway_line_ja=a.railway_line_ja, railway_line_en=a.railway_line_en, railway_line_zh=a.railway_line_zh, service_route_ja=a.service_route_ja, service_route_en=a.service_route_en, service_route_zh=a.service_route_zh, type=a.type from "+tb_to_tileset_polygon+" a where a.name=b.name"
    print(qry)
    cur.execute(qry)
    connection.commit()

    qry = "update "+tb_to_tileset_point+" set gis_service_route=array" + \
        str([''])+" where gis_service_route is null"
    print(qry)
    cur.execute(qry)
    connection.commit()

    qry = "update "+tb_to_tileset_point+" set gis_railway_line=array" + \
        str([''])+" where gis_railway_line is null"
    print(qry)
    cur.execute(qry)
    connection.commit()


create_new_point_table()
add_gis_point()
update_gis_railway_line()
update_gis_service_route()
update_gis_type()
insert_centroid()
update_everything()
