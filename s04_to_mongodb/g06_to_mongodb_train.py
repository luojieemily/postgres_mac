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
tb_train='train'
tb_service_route='service_route'
tb_to_mongodb_train='to_mongodb_train_'+config['version_code']

to_file_path=config['temp_file_path']
to_file_name='to_mongodb_train_'+config['version_code']+'.json'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()


def create_new_to_mongodb_train_table():
    qry="DROP TABLE IF EXISTS "+tb_to_mongodb_train
    cur.execute(qry)
    connection.commit()

    qry="CREATE TABLE "+tb_to_mongodb_train+" as select stations as stations_ja, detail_info, runday, dep, arr from "+tb_train
    cur.execute(qry)
    connection.commit()  
    
    qry="CREATE INDEX idx_train ON "+tb_to_mongodb_train+" (stations_ja, dep, arr);"
    cur.execute(qry)
    connection.commit()
    
    qry="ALTER TABLE "+tb_to_mongodb_train  +"  ADD COLUMN IF NOT EXISTS route_id text, ADD COLUMN IF NOT EXISTS railwaylines text[], ADD COLUMN IF NOT EXISTS serviceroutes text[], ADD COLUMN IF NOT EXISTS train_type text, ADD COLUMN IF NOT EXISTS stations_en text[], ADD COLUMN IF NOT EXISTS stations_zh text[], ADD COLUMN IF NOT EXISTS depm integer, ADD COLUMN IF NOT EXISTS arrm integer"
    cur.execute(qry)
    connection.commit()  
    
def update_cols_from_dict_train():
    dict_rk_direct_railwaylines={}
    dict_rk_direct_serviceroutes={}
    dict_rk_direct_type={}
    
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
                
    qry="select ra_keys, type from "+tb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        the_type=each[1]
        for ra_key in ra_keys:
            dict_rk_direct_type[ra_key]=the_type
    
    qry="select stations, isshinkansen, route_id, ra_keys from ctl_stations"
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        stations=each[0]
        isshinkansen=each[1]
        route_id=each[2]
        ra_keys=each[3]
        dict_type={}
        railwaylines=[]
        serviceroutes=[]
        for rk in ra_keys:
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
            if rk in dict_rk_direct_type:
                the_type=dict_rk_direct_type[rk]
                if the_type not in dict_type:
                    dict_type[the_type]=0
                dict_type[the_type]+=1
        if isshinkansen:
            train_type='shinkansen'
        else:
            max_count=0
            train_type=''
            for t in dict_type:
                if dict_type[t]>max_count:
                    train_type=t
                    max_count=dict_type[t]
        qry="update "+tb_to_mongodb_train+" set route_id='"+route_id+"', railwaylines=array"+str(railwaylines)+", serviceroutes=array"+str(serviceroutes)+", train_type='"+train_type+"' where stations_ja=array"+str(stations)
        print(qry)
        cur.execute(qry)
        connection.commit()  
        
def update_cols_others():
    dict_polygon={}
    qry="select name_ja, name_en,name_zh from "+tb_polygon
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        name_ja=each[0]
        name_en=each[1]
        name_zh=each[2]
        dict_polygon[name_ja]=[name_en, name_zh]
    qry="select stations, dep, arr from "+tb_train
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        stations_ja=each[0]
        dep=each[1]
        arr=each[2]
        dep_hour=int(dep[0].split(":")[0])
        dep_min=int(dep[0].split(":")[1])
        arr_hour=int(arr[-1].split(":")[0])
        arr_min=int(arr[-1].split(":")[1])
        depm=dep_hour*60+dep_min
        arrm=arr_hour*60+arr_min
        stations_en=[]
        stations_zh=[]
        for station in stations_ja:
            if station in dict_polygon:
                stations_en.append(dict_polygon[station][0])
                stations_zh.append(dict_polygon[station][1])
            else:
                stations_en.append(station)
                stations_zh.append(station)
        qry="update "+tb_to_mongodb_train+" set stations_en=array"+str(stations_en)+", stations_zh=array"+str(stations_zh)+", depm="+str(depm)+", arrm="+str(arrm)+" where stations_ja=array"+str(stations_ja)+" and dep=array"+str(dep)+" and arr=array"+str(arr)
        print(qry)
        cur.execute(qry)
        connection.commit() 
        
def change_special_character():
    qry=r"select detail_info, stations_ja, dep, arr from "+tb_to_mongodb_train+" where array_to_string(detail_info,',') like '%\\u3000%'"
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        detail_info=each[0]
        new_detail_info=[]
        stations_ja=each[1]
        dep=each[2]
        arr=each[3]
        for d in detail_info:
            detail=d.replace('\\u3000','')
            new_detail_info.append(detail)
        qry="update "+tb_to_mongodb_train+" set detail_info = array"+str(new_detail_info)+" where stations_ja=array"+str(stations_ja)+" and dep=array"+str(dep)+" and arr=array"+str(arr)
        print(qry)
        cur.execute(qry)
        connection.commit() 
        
def update_icon_image():
    qry="ALTER TABLE "+tb_to_mongodb_train  +"  ADD COLUMN IF NOT EXISTS icon_image text"
    cur.execute(qry)
    connection.commit()  
    qry="select train_type, detail_info from "+tb_to_mongodb_train +" group by train_type, detail_info"
    cur.execute(qry)
    results = cur.fetchall()
    for each in results: 
        train_type=each[0]
        detail_info=each[1]
        detail=each[1][0]
        if train_type=='shinkansen':
            icon_image='shinkansen-'+detail[4]
        elif train_type=='rail_JR':
            icon_image='rail_jr-'+detail[0]
        elif train_type=='rail_private':
            icon_image="rail_private-"+detail[0]
        elif train_type=='subway':
            icon_image='subway-'+detail[0]
        elif train_type=='tram':
            icon_image='tram-'+detail[0]
        else:
            icon_image="others-"+detail[0]
        qry="update "+tb_to_mongodb_train+" set icon_image='"+icon_image+"' where train_type='"+train_type+"' and detail_info=array"+str(detail_info)
        print(qry)
        cur.execute(qry)
        connection.commit()  
                
def gen_mongo_train_json():
    qry="SELECT json_build_object('stations_ja',stations_ja, 'stations_en',stations_en,'stations_zh',stations_zh,'detail_info',detail_info, 'runday',runday, 'dep', dep, 'arr',arr,'route_id', route_id, 'railwaylines',railwaylines,'serviceroutes',serviceroutes, 'train_type', train_type, 'depm', depm, 'arrm', arrm, 'icon_image', icon_image ) from (select * from "+tb_to_mongodb_train+") row"
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
    
    subprocess.call ('mongoimport --uri "mongodb+srv://'+mongodb_user+':'+mongodb_password+'@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority"  --collection train_'+config['version_code']+' --drop --file ' +to_file_path+'/'+to_file_name+' --jsonArray --maintainInsertionOrder --batchSize 1', shell=True )
    

# create_new_to_mongodb_train_table()
# update_cols_from_dict_train()
# update_cols_others()
# change_special_character()
# update_icon_image()
gen_mongo_train_json()