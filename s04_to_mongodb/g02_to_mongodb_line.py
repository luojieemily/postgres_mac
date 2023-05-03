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
tb_service_route='service_route'
tb_railway_line='railway_line'
tb_to_mongodb_line='to_mongodb_line_'+config['version_code']
to_file_path=config['temp_file_path']
to_file_name='to_mongodb_line_'+config['version_code']+'.json'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()


def create_new_to_mongodb_line_table():
    qry="DROP TABLE IF EXISTS "+tb_to_mongodb_line
    cur.execute(qry)
    connection.commit()

    qry="CREATE TABLE "+tb_to_mongodb_line+" as select ra_key, ST_SimplifyPreserveTopology(way,5) as way from "+tb_line
    cur.execute(qry)
    connection.commit()  
    
    qry="ALTER TABLE "+tb_to_mongodb_line  +" ADD COLUMN IF NOT EXISTS railwaylines text[], ADD COLUMN IF NOT EXISTS serviceroutes text[], ADD COLUMN IF NOT EXISTS stations text[]"
    cur.execute(qry)
    connection.commit()  
    
def update_cols():
    dict_rk_related_ra_keys={}
    dict_rk_related_points={}
    dict_rk_direct_railwaylines={}
    dict_rk_direct_serviceroutes={}
    qry="select ra_keys, points from ctl_guide"
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        points=each[1]
        for ra_key in ra_keys:
            if ra_key not in dict_rk_related_ra_keys:
                dict_rk_related_ra_keys[ra_key]=[]
            if ra_key not in dict_rk_related_points:
                dict_rk_related_points[ra_key]=[]
            for r in ra_keys:
                if r not in dict_rk_related_ra_keys[ra_key]:
                    dict_rk_related_ra_keys[ra_key].append(r)
            for p in points:
                if p not in dict_rk_related_points[ra_key]:
                    dict_rk_related_points[ra_key].append(p)
    qry="select ra_key, railway_line from "+tb_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_key=each[0]
        railwayline=each[1].split('(')[0]
        if ra_key not in dict_rk_direct_railwaylines:
            dict_rk_direct_railwaylines[ra_key]=[]
        if railwayline not in dict_rk_direct_railwaylines[ra_key]:
            dict_rk_direct_railwaylines[ra_key].append(railwayline)

    qry="select ra_keys, service_route_ja from "+tb_service_route
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        serviceroute=each[1].split('(')[0]
        for ra_key in ra_keys:
            if ra_key not in dict_rk_direct_serviceroutes:
                dict_rk_direct_serviceroutes[ra_key]=[]
            if serviceroute not in dict_rk_direct_serviceroutes[ra_key]:
                dict_rk_direct_serviceroutes[ra_key].append(serviceroute)
                
    for ra_key in dict_rk_related_ra_keys:
        stations=dict_rk_related_points[ra_key]
        railwaylines=[]
        serviceroutes=[]
        for rk in dict_rk_related_ra_keys[ra_key]:
            if rk in dict_rk_direct_railwaylines:
                lines=dict_rk_direct_railwaylines[rk]
                for l in lines:
                    if l not in railwaylines:
                        railwaylines.append(l)
            if rk in dict_rk_direct_serviceroutes:
                routes=dict_rk_direct_serviceroutes[rk]        
                for r in routes:
                    if r not in serviceroutes:
                        serviceroutes.append(r)
        qry="update "+tb_to_mongodb_line+" set railwaylines=array"+str(railwaylines)+", serviceroutes=array"+str(serviceroutes)+", stations=array"+str(stations)+" where ra_key='"+ra_key+"'"
        cur.execute(qry)
        connection.commit()  
        
def gen_mongo_line_json():
    qry="SELECT json_build_object('ra_key',ra_key, 'railwaylines',railwaylines,'serviceroutes',serviceroutes, 'stations', stations, 'way', ST_AsGeoJSON(geog, 5)::jsonb) from (select * ,ST_Transform( way, 4326) as geog from "+tb_to_mongodb_line+") row"
    cur.execute(qry)
    results = cur.fetchall()
    with open(to_file_path+'/'+to_file_name, 'w') as f:
        f.write('[')
        for row in results:
            f.write( unicodedata.normalize('NFKC', str(row[0]).replace("'",'"').replace("None", '""')) )
            # 当原数据库里某一列为空时，导出会是None，没有引号，这个导致import to MongoDB出错，需替换为“”
            
            if row != results[-1]:
                f.write(',')
        # 最后一个object后面不应有逗号
        f.write(']')
            # 导入MongoDB的原始文件需要是一个列表[]
            
    
    with open(to_file_path+'/'+to_file_name) as f:
        newText=f.read().replace('"{','{').replace('}"','}').replace('"[','[').replace(']"',']').replace('True','true').replace('False','false')

    with open(to_file_path+'/'+to_file_name, 'w') as f:
        f.write(newText)
    
    subprocess.call ('mongoimport --uri "mongodb+srv://'+mongodb_user+':'+mongodb_password+'@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority"  --collection line_'+config['version_code']+' --drop --file ' +to_file_path+'/'+to_file_name+' --jsonArray --maintainInsertionOrder --batchSize 1', shell=True )


create_new_to_mongodb_line_table()
update_cols()
gen_mongo_line_json()