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
tb_service_route='service_route'
tb_to_mongodb_service_route='to_mongodb_service_route_'+config['version_code']

to_file_path=config['temp_file_path']
to_file_name='to_mongodb_serviceroute_'+config['version_code']+'.json'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()


def create_new_to_mongodb_service_route_table():
    qry="DROP TABLE IF EXISTS "+tb_to_mongodb_service_route
    cur.execute(qry)
    connection.commit()

    qry="CREATE TABLE "+tb_to_mongodb_service_route+" as select SPLIT_PART(service_route_ja,'(',1) as service_route_ja, SPLIT_PART(service_route_en,'(',1) as service_route_en, SPLIT_PART(service_route_zh,'(',1) as service_route_zh, service_route_ja as official_name_ja, service_route_en as official_name_en, service_route_zh as official_name_zh, stations as stations_ja,company as company_ja, type, ra_keys,  priority from "+tb_service_route
    cur.execute(qry)
    connection.commit()  
    

def complete_company_stations_prefecture():
    qry="alter table "+tb_to_mongodb_service_route+" add column company_en text, add column company_zh text, add column stations_en text[], add column stations_zh text[], add column prefecture_ja text[], add column prefecture_en text[], add column prefecture_zh text[]"
    cur.execute(qry)
    connection.commit()
    
    qry='select company_ja, company_en, company_zh from ctl_company'
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        company_ja=each[0]
        company_en=each[1]
        company_zh=each[2]
        qry="update "+tb_to_mongodb_service_route+" set company_en='"+company_en+"', company_zh='"+company_zh+"' where company_ja='"+company_ja+"'"
        cur.execute(qry)
        connection.commit()
    
    dict_prefecture={}
    qry="select prefecture_ja, prefecture_en, prefecture_zh from ctl_prefecture"    
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        prefecture_ja=each[0]
        prefecture_en=each[1]
        prefecture_zh=each[2]
        dict_prefecture[prefecture_ja]=[prefecture_en,prefecture_zh]
    
    dict_point={}    
    qry="select name_ja, name_en, name_zh, prefecture from "+tb_polygon
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        name_ja=each[0]
        name_en=each[1]
        name_zh=each[2]
        prefecture_ja=each[3]
        prefecture_en=dict_prefecture[prefecture_ja][0]
        prefecture_zh=dict_prefecture[prefecture_ja][1]
 
        dict_point[name_ja]=[name_en, name_zh, prefecture_ja,prefecture_en,prefecture_zh]
    
    qry="select stations_ja from "+tb_to_mongodb_service_route
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        stations_ja=each[0]
        stations_en=[]
        stations_zh=[]
        prefecture_ja=[]
        prefecture_en=[]
        prefecture_zh=[]
        for p in stations_ja:
            stations_en.append(dict_point[p][0])
            stations_zh.append(dict_point[p][1])
            prefecture_ja.append(dict_point[p][2])
            prefecture_en.append(dict_point[p][3])
            prefecture_zh.append(dict_point[p][4])
        qry="update "+tb_to_mongodb_service_route+" set stations_en=array"+str(stations_en)+", stations_zh=array"+str(stations_zh)+", prefecture_ja=array"+str(prefecture_ja)+", prefecture_en=array"+str(prefecture_en)+", prefecture_zh=array"+str(prefecture_zh)+" where stations_ja=array"+str(stations_ja)
      
        cur.execute(qry)
        connection.commit()
     
def add_acc_distance():
    qry="ALTER TABLE "+tb_to_mongodb_service_route  +" ADD COLUMN IF NOT EXISTS acc_distance numeric[]"
    cur.execute(qry)
    connection.commit()
    dict_ra_key={}
    qry="select ra_key,distance from "+tb_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        ra_key=each[0]
        distance=each[1]
        dict_ra_key[ra_key]=distance
    qry="select stations, ra_keys, points from "+tb_service_route
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        station_list=each[0]
        ra_keys=each[1]
        points=each[2]
        ra_key_distance=list(map(lambda x:dict_ra_key[x],ra_keys))
        list_acc_distance = []
        idx=0
        for station in station_list:
            idx=points.index(station, idx)
            if idx>0:
                acc_distance=str(round(sum(ra_key_distance[:idx]),1))
                list_acc_distance.append(acc_distance)
            else:
                list_acc_distance.append('0')
     
        qry="update "+tb_to_mongodb_service_route+" set acc_distance=array"+str(list_acc_distance).replace("'","")+" where ra_keys=array"+str(ra_keys)+" and stations_ja=array"+str(station_list)

        cur.execute(qry)
        connection.commit()
    
    
def add_including_service_route():
    qry="ALTER TABLE "+tb_to_mongodb_service_route  +" ADD COLUMN IF NOT EXISTS including_service_route_ja text[], ADD COLUMN IF NOT EXISTS including_service_route_en text[],ADD COLUMN IF NOT EXISTS including_service_route_zh text[]"
    cur.execute(qry)
    connection.commit()
    dict_station={}
    dict_service_route={}
    qry="select stations, service_route_ja, service_route_en, service_route_zh from "+tb_service_route +" order by array_length (stations, 1) desc"
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        stations=each[0]
        service_route_ja=each[1].split("(")[0]
        service_route_en=each[2].split("(")[0]
        service_route_zh=each[3].split("(")[0]
        for station in stations:
            if station not in dict_station:
                dict_station[station]=[]
            if service_route_ja not in dict_station[station]:
                dict_station[station].append(service_route_ja)
        if service_route_ja not in dict_service_route:
            dict_service_route[service_route_ja]=[service_route_en,service_route_zh]
       
    qry="select stations_ja from "+tb_to_mongodb_service_route
    cur.execute(qry)
    r=cur.fetchall()

    for each in r:  
        stations_ja=each[0]
        list_service_route_ja=[]
        list_service_route_en=[]
        list_service_route_zh=[]
        for point in stations_ja:
            if point in dict_station:
                service_route_ja=dict_station[point]
                sorted_service_route_ja=[]
                sorted_service_route_en=[]
                sorted_service_route_zh=[]
                for line in service_route_ja:
                    if '新幹線' in line:
                        sorted_service_route_ja.append(line)
                for line in service_route_ja:
                    if '新幹線' not in line and 'JR' in line:
                        sorted_service_route_ja.append(line)
                for line in service_route_ja:
                    if 'JR' not in line and line not in sorted_service_route_ja:
                        sorted_service_route_ja.append(line)
                        left_2_word=line[:2]
                        for l in service_route_ja:
                            if 'JR' not in l and l not in sorted_service_route_ja and l[:2]==left_2_word:
                                sorted_service_route_ja.append(l)
                        
                for line in sorted_service_route_ja:
                    sorted_service_route_en.append(dict_service_route[line][0])
                    sorted_service_route_zh.append(dict_service_route[line][1])
                str_service_route_ja=str(sorted_service_route_ja).replace('[','').replace(']','').replace("'","").replace(" ","")
                str_service_route_en=str(sorted_service_route_en).replace('[','').replace(']','').replace("'","")
                str_service_route_zh=str(sorted_service_route_zh).replace('[','').replace(']','').replace("'","").replace(" ","")
                list_service_route_ja.append(str_service_route_ja)
                list_service_route_en.append(str_service_route_en)
                list_service_route_zh.append(str_service_route_zh)
            else:
                list_service_route_ja.append('')
                list_service_route_en.append('')
                list_service_route_zh.append('')
        qry="update "+tb_to_mongodb_service_route+" set including_service_route_ja=array"+str(list_service_route_ja)+", including_service_route_en=array"+str(list_service_route_en)+", including_service_route_zh=array"+str(list_service_route_zh)+" where stations_ja=array"+str(stations_ja)
        cur.execute(qry)
        connection.commit()
                

def add_way_bounds():
    qry="ALTER TABLE "+tb_to_mongodb_service_route  +" ADD COLUMN IF NOT EXISTS way_bounds geometry"
    cur.execute(qry)
    connection.commit()
    qry="select ra_keys from "+tb_to_mongodb_service_route
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        qry="update "+tb_to_mongodb_service_route+" set way_bounds=(select ST_Envelope(ST_LineMerge(ST_Union(c.way))) from (select way from "+tb_line+" a where a.ra_key =ANY ((select ra_keys from "+tb_to_mongodb_service_route+" b where b.ra_keys = array"+str(ra_keys)+")::text[]) ) as c) where ra_keys=array"+str(ra_keys)
        cur.execute(qry)
        connection.commit()
        
def add_including_railway_line():
    dict_line = {}
    statement = "select ra_key, railway_line from "+tb_line +" where railway_line is not null"
    cur.execute(statement)
    results = cur.fetchall()
    for each in results:
        ra_key=each[0]
        gis_line=each[1]
        dict_line[ra_key]=gis_line
        
    dict_translate_railway_line={}   
    qry="select railway_line_ja, ra_keys,railway_line_en, railway_line_zh from "+tb_railway_line
    cur.execute(qry)
    results = cur.fetchall()
    for each in results:
        gis_line=each[0]
        ra_keys=each[1]
        railway_line_en=each[2]
        railway_line_zh=each[3]
        if gis_line not in dict_translate_railway_line:
            dict_translate_railway_line[gis_line]=[railway_line_en,railway_line_zh]
        for ra_key in ra_keys:
            if ra_key not in dict_line:
                dict_line[ra_key]=gis_line
    
    qry="ALTER TABLE "+tb_to_mongodb_service_route  +" ADD COLUMN IF NOT EXISTS including_railway_line_ja text[], ADD COLUMN IF NOT EXISTS including_railway_line_en text[],ADD COLUMN IF NOT EXISTS including_railway_line_zh text[]"
    cur.execute(qry)
    connection.commit()
    
    statement = "Select  service_route_ja, stations, ra_keys, points from "+tb_service_route
    cur.execute(statement)
    results = cur.fetchall()
    for each in results:
        service_route = each[0]
        station_list= each[1]
        ra_keys=each[2]
        list_ra_key_gis_line=list(map(lambda x:dict_line[x],ra_keys))
        points=each[3]
        list_gis_line = []
        list_gis_line_en=[]
        list_gis_line_zh=[]
        pre_idx=0
        for station in station_list:
            idx=points.index(station, pre_idx)
            if idx>0:
                this_list=[]
                this_list_en=[]
                this_list_zh=[]
                ra_key_gis_line=list_ra_key_gis_line[pre_idx:idx]
                for gis_line in ra_key_gis_line:
                    if gis_line not in this_list:
                        this_list.append(gis_line)
                        this_list_en.append(dict_translate_railway_line[gis_line][0])
                        this_list_zh.append(dict_translate_railway_line[gis_line][1])
                list_gis_line.append(str(this_list).replace('[','').replace(']','').replace("'",''))
                list_gis_line_en.append(str(this_list_en).replace('[','').replace(']','').replace("'",''))
                list_gis_line_zh.append(str(this_list_zh).replace('[','').replace(']','').replace("'",''))
            else:
                list_gis_line.append('')
                list_gis_line_en.append('')
                list_gis_line_zh.append('')
            pre_idx=idx
    
        statement = "update "+tb_to_mongodb_service_route+" set including_railway_line_ja=array"+str(list_gis_line)+", including_railway_line_en=array"+str(list_gis_line_en)+", including_railway_line_zh=array"+str(list_gis_line_zh)+" where official_name_ja='"+service_route+"' and ra_keys=array"+str(ra_keys)
        cur.execute(statement)
        connection.commit()
    
def gen_mongo_service_route_json():
    qry = "SELECT json_build_object('serviceroute_ja',service_route_ja, 'serviceroute_en',service_route_en, 'serviceroute_zh',service_route_zh,'company_ja', company_ja, 'company_en', company_en, 'company_zh', company_zh, 'official_name_ja', official_name_ja, 'official_name_en', official_name_en, 'official_name_zh', official_name_zh, 'type', type, 'ra_keys', ra_keys, 'stations_ja', stations_ja, 'stations_en', stations_en, 'stations_zh', stations_zh, 'prefecture_ja', prefecture_ja, 'prefecture_en', prefecture_en, 'prefecture_zh', prefecture_zh, 'priority', priority, 'including_railway_line_ja', including_railway_line_ja, 'including_railway_line_en', including_railway_line_en, 'including_railway_line_zh', including_railway_line_zh, 'including_service_route_ja', including_service_route_ja, 'including_service_route_en', including_service_route_en, 'including_service_route_zh', including_service_route_zh, 'acc_distance', acc_distance, 'way_bounds',ST_AsGeoJSON(geog, 5)::jsonb) from (select * ,ST_Transform( way_BOUNDS, 4326) as geog from "+tb_to_mongodb_service_route+") row"
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
    
    subprocess.call ('mongoimport --uri "mongodb+srv://'+mongodb_user+':'+mongodb_password+'@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority"  --collection serviceroute_'+config['version_code']+' --drop --file ' +to_file_path+'/'+to_file_name+' --jsonArray --maintainInsertionOrder --batchSize 1', shell=True )
   

    
create_new_to_mongodb_service_route_table()
complete_company_stations_prefecture()
add_acc_distance()
add_including_service_route()
add_way_bounds()
add_including_railway_line()
gen_mongo_service_route_json()