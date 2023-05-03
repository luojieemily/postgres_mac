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
tb_from='to_tileset_polygon_'+config['version_code']
tb_compare="polygon"
to_file_path=config['temp_file_path']
to_file_name='stations'+'.js'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()

qry = "SELECT json_build_object('name_ja',name,'name_en',(select name_en from "+tb_compare+" b where a.name=b.name_ja) , 'name_zh', (select name_zh from "+tb_compare+" b where a.name=b.name_ja)) from "+tb_from+" a where  a.railway_line_ja is not null order by priority"
cur.execute(qry)
results=cur.fetchall()

with open(to_file_path+'/'+to_file_name, 'w') as f:
    f.write('export default [')
    for row in results:
        f.write( str(row[0]).replace("'",'"').replace("None", '""'))
        # # 当原数据库里某一列为空时，导出会是None，没有引号，这个导致import to MongoDB出错，需替换为“”
        if row != results[-1]:
            f.write(',')
    # 最后一个object后面不应有逗号
    f.write(']')

# for lan in file_list:
    # to_file_name='station_'+lan+".js"
    # qry = "SELECT json_build_object('label', name_"+lan+",'station',name_ja) from "+tb_polygon
    # cur.execute(qry)
    # results=cur.fetchall()

    # with open(to_file_path+'/'+to_file_name, 'w') as f:
    #     f.write('export default [')
    #     for row in results:
    #         f.write( str(row[0]).replace("'",'"').replace("None", '""'))
    #         # # 当原数据库里某一列为空时，导出会是None，没有引号，这个导致import to MongoDB出错，需替换为“”
    #         if row != results[-1]:
    #             f.write(',')
    #     # 最后一个object后面不应有逗号
    #     f.write(']')