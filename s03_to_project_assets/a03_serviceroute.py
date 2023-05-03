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
tb_from='service_route'
to_file_path=config['temp_file_path']
to_file_name='serviceroutes'+'.js'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()

qry="drop table if exists temp"
cur.execute(qry)
connection.commit()

qry="create table temp as select split_part(service_route_JA,'(',1) as service_route_ja, split_part(service_route_en,'(',1) as service_route_en, split_part(service_route_zh,'(',1) as service_route_zh, COMPANY, RA_KEYS FROM "+tb_from+"  ORDER BY substring(service_route_ZH, '新干线'), substring(company, 'JR'), array_length(ra_keys, 1) desc "
cur.execute(qry)
connection.commit()

qry="DELETE FROM temp T1 USING temp T2 WHERE  T1.ctid < T2.ctid AND T1.service_route_JA = T2.service_route_JA;"
cur.execute(qry)
connection.commit()


qry = "SELECT json_build_object('name_ja',service_route_ja, 'name_en', service_route_en, 'name_zh', service_route_zh) from temp"
cur.execute(qry)
results=cur.fetchall()

with open(to_file_path+'/'+to_file_name, 'w') as f:
    f.write('export default [')
    for row in results:
        f.write( str(row[0]).replace("'",'"').replace("None", '""').replace("～","~"))
        # # 当原数据库里某一列为空时，导出会是None，没有引号，这个导致import to MongoDB出错，需替换为“”
        if row != results[-1]:
            f.write(',')
    # 最后一个object后面不应有逗号
    f.write(']')

