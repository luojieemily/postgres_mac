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
to_file_path = config['temp_file_path']
to_file_name = 'polygon_'+config['version_code']+'.geojson'
tb_from = 'to_tileset_polygon_'+config['version_code']
tb_to_tileset_point = 'to_tileset_point_'+config['version_code']
select_column = "name, name_ja, name_en, name_zh, prefecture_ja, prefecture_en, prefecture_zh, ARRAY_TO_STRING(railway_line_ja,',') as railway_line_ja, ARRAY_TO_STRING(railway_line_en,',') as railway_line_en, ARRAY_TO_STRING(railway_line_zh,',') as railway_line_zh, ARRAY_TO_STRING(service_route_ja,',') as service_route_ja, ARRAY_TO_STRING(service_route_en,',') as service_route_en, ARRAY_TO_STRING(service_route_zh,',') as service_route_zh,priority, array_to_string(type,',') as type"


connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()

qry = "SELECT jsonb_build_object('type',  'FeatureCollection', 'features', jsonb_agg(feature))  FROM (SELECT jsonb_build_object('type', 'Feature', 'geometry',  ST_AsGeoJSON(geog, 6)::jsonb,   'properties', to_jsonb(row)- 'geog' ) AS feature FROM (SELECT " + \
    select_column+", ST_Transform( way, 4326) as geog  FROM "+tb_from + \
    " where railway_line_ja is not null and name in (select name from " + \
    tb_to_tileset_point+" where gis_type='gis_centroid') order by priority ) row) features;"
cur.execute(qry)
results = cur.fetchall()

with open(to_file_path+'/'+to_file_name, 'w') as f:
    # f.write('[')
    for row in results:
        f.write(str(row[0]).replace("'", '"').replace("None", '""'))
        # # 当原数据库里某一列为空时，导出会是None，没有引号，这个导致import to MongoDB出错，需替换为“”
        # if row != results[-1]:
        #     f.write(',')
    # 最后一个object后面不应有逗号
    # f.write(']')
