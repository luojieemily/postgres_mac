import configparser
import psycopg2
from configparser import ConfigParser
config_parser=ConfigParser()
config_parser.read('../config.cfg')
config=config_parser['DEFAULT']
database=config['database']+'_'+config['version_code']
hostname=config['hostname']
username=config['username']
password=config['password']
tb_to_tileset_point='to_tileset_point_'+config['version_code']
tb_to_tileset_polygon='to_tileset_polygon_'+config['version_code']


connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()

## based on to_tileset_polygon

def create_new_point_table():
    qry="DROP TABLE IF EXISTS "+tb_to_tileset_point
    cur.execute(qry)
    connection.commit()
    
    qry="CREATE TABLE "+tb_to_tileset_point+" as select st_centroid(way) as way, priority, name, name_ja, name_en, name_zh, prefecture_ja,  prefecture_en, prefecture_zh, railway_line_ja, railway_line_en, railway_line_zh, service_route_ja, service_route_en, service_route_zh, type_shinkansen , type_rail_jr , type_rail_private, type_subway, type_tram , type_others from "+tb_to_tileset_polygon
    cur.execute(qry)
    connection.commit()
   
create_new_point_table()
