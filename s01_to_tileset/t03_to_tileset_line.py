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
tb_to_tileset_line = 'to_tileset_line_'+config['version_code']
tb_line = 'line'
tb_railway_line = 'railway_line'
tb_service_route = 'service_route'

connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()


def create_new_line_table():
    qry = "DROP TABLE IF EXISTS "+tb_to_tileset_line
    cur.execute(qry)
    connection.commit()

    qry = "CREATE TABLE "+tb_to_tileset_line + \
        " as select way, ra_key, railway_line as railway_line_ja from "+tb_line
    cur.execute(qry)
    connection.commit()


def add_railway_line():
    qry = "alter table "+tb_to_tileset_line + \
        " add column railway_line_en text, add column railway_line_zh text"
    cur.execute(qry)
    connection.commit()
    qry = "select railway_line_ja, ra_keys from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        railway_line = each[0]
        ra_keys = each[1]

        for l in ra_keys:
            qry = "update "+tb_to_tileset_line+" set railway_line_ja='" + \
                railway_line+"' where ra_key='"+l+"'"
            cur.execute(qry)
            connection.commit()


def add_en_zh_for_railway_line():

    qry = "select railway_line_ja, railway_line_en, railway_line_zh from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        railway_line_ja = each[0]
        railway_line_en = each[1]
        railway_line_cn = each[2]
        qry = "update "+tb_to_tileset_line+" set railway_line_en='"+railway_line_en + \
            "', railway_line_zh='"+railway_line_cn + \
            "' where railway_line_ja='"+railway_line_ja+"'"
        cur.execute(qry)
        connection.commit()


def add_type():
    qry = "alter table "+tb_to_tileset_line + \
        " add column type text"  # 以ra_key为单位的line，type肯定是唯一的
    cur.execute(qry)
    connection.commit()
    dict_rakey_type = {}  # dict['kure_001']='rail_jr

    qry = "select type, ra_keys from "+tb_railway_line
    cur.execute(qry)
    r = cur.fetchall()
    for each in r:
        ra_keys = each[1]
        rakey_type = ''
        if 'shinkansen' == each[0]:
            rakey_type = 'shinkansen'
        elif 'rail_JR' == each[0]:
            rakey_type = 'rail_jr'
        elif 'rail_private' == each[0]:
            rakey_type = 'rail_private'
        elif 'subway' == each[0]:
            rakey_type = 'subway'
        elif 'tram' == each[0]:
            rakey_type = 'tram'
        else:
            rakey_type = 'others'
        for rakey in ra_keys:
            if rakey not in dict_rakey_type:
                dict_rakey_type[rakey] = rakey_type
    for k in dict_rakey_type:
        qry = "update " + tb_to_tileset_line+" set type='" + \
            dict_rakey_type[k]+"' where ra_key='"+k+"'"
        cur.execute(qry)
        connection.commit()
    qry = "select ra_key, railway_line_ja from " + \
        tb_to_tileset_line + " where type is null"
    cur.execute(qry)
    result = cur.fetchall()
    for each in result:
        ra_key = each[0]
        railway_line_ja = each[1]

        q = "select type from "+tb_railway_line + \
            " where railway_line_ja='"+railway_line_ja+"'"
        cur.execute(q)
        re = cur.fetchall()
        if len(re) > 0:
            line_type = ''
            if 'shinkansen' == re[0][0]:
                line_type = 'shinkansen'
            elif 'rail_JR' == re[0][0]:
                line_type = 'rail_jr'
            elif 'rail_private' == re[0][0]:
                line_type = 'rail_private'
            elif 'subway' == re[0][0]:
                line_type = 'subway'
            elif 'tram' == re[0][0]:
                line_type = 'tram'
            else:
                line_type = 'others'
            qr = "update "+tb_to_tileset_line+" set type='" + \
                line_type+"' where ra_key='"+ra_key+"'"
            cur.execute(qr)
            connection.commit()


create_new_line_table()
add_railway_line()
add_en_zh_for_railway_line()
add_type()
