import configparser
import psycopg2
from configparser import ConfigParser
import unicodedata
import subprocess
config_parser=ConfigParser()
config_parser.read('../config.cfg')
config=config_parser['DEFAULT']
database=config['database']+'_'+config['version_code']
hostname=config['hostname']
username=config['username']
password=config['password']

mongodb_user=config['mongodb_user']
mongodb_password=config['mongodb_password']
mongodb_database=config['mongodb_database']

tb_polygon='polygon'
tb_line='line'
tb_railway_line='railway_line'
tb_ctl_dict_train='ctl_stations'
tb_ctl_route_guide='ctl_guide'
tb_train='c_220123_train_disney_resort_line'
tb_service_route='service_route'
tb_to_mongodb_train='to_mongodb_train_'+config['version_code']+'_temp'

to_file_path=config['temp_file_path']
to_file_name='to_mongodb_train_'+config['version_code']+'.json'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()

dict_line={}
qry="select distance, ra_key from line"
cur.execute(qry)
results = cur.fetchall()
for each in results:
    distance=each[0]*1
    ra_key=each[1]
    dict_line[ra_key]=distance

dict_main_line={}
qry="select ra_keys, railway_line_ja from railway_line where official_name_ja like '%本線%' AND TYPE !='tram'"
cur.execute(qry)
results = cur.fetchall()
for each in results:
    ra_keys=each[0]
    railway_line_ja=each[1]
    if railway_line_ja not in dict_main_line:
        dict_main_line[railway_line_ja]=[]
    for k in ra_keys:
        if k not in dict_main_line[railway_line_ja]:
            dict_main_line[railway_line_ja].append(k)
dict_main_line_length={}                
for l in dict_main_line:
    dict_main_line_length[l]=0
    for d in dict_main_line[l]:
        dict_main_line_length[l]+=dict_line[d]
        
sortedDict = dict(sorted(dict_main_line_length.items(), key=lambda item: item[1]))
        
print(sortedDict)
    
    