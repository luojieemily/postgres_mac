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
tb_to_mongodb_railway_line='to_mongodb_railway_line_'+config['version_code']

to_file_path=config['temp_file_path']
to_file_name='to_mongodb_railwayline_'+config['version_code']+'.json'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()


def create_new_to_mongodb_railway_line_table():
    qry="DROP TABLE IF EXISTS "+tb_to_mongodb_railway_line
    cur.execute(qry)
    connection.commit()

    qry="CREATE TABLE "+tb_to_mongodb_railway_line+" as select railway_line_ja, railway_line_en, railway_line_zh, company as company_ja, type, official_name_ja, official_name_en, official_name_zh, ra_keys, points as points_ja, priority from "+tb_railway_line
    cur.execute(qry)
    connection.commit()  
    

def complete_company_points_prefecture():
    qry="alter table "+tb_to_mongodb_railway_line+" add column company_en text, add column company_zh text, add column points_en text[], add column points_zh text[], add column prefecture_ja text[], add column prefecture_en text[], add column prefecture_zh text[]"
    cur.execute(qry)
    connection.commit()
    
    qry='select company_ja, company_en, company_zh from ctl_company'
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        company_ja=each[0]
        company_en=each[1]
        company_zh=each[2]
        qry="update "+tb_to_mongodb_railway_line+" set company_en='"+company_en+"', company_zh='"+company_zh+"' where company_ja='"+company_ja+"'"
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
    
    qry="select points_ja from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        points_ja=each[0]
        points_en=[]
        points_zh=[]
        prefecture_ja=[]
        prefecture_en=[]
        prefecture_zh=[]
        for p in points_ja:
            points_en.append(dict_point[p][0])
            points_zh.append(dict_point[p][1])
            prefecture_ja.append(dict_point[p][2])
            prefecture_en.append(dict_point[p][3])
            prefecture_zh.append(dict_point[p][4])
        qry="update "+tb_to_mongodb_railway_line+" set points_en=array"+str(points_en)+", points_zh=array"+str(points_zh)+", prefecture_ja=array"+str(prefecture_ja)+", prefecture_en=array"+str(prefecture_en)+", prefecture_zh=array"+str(prefecture_zh)+" where points_ja=array"+str(points_ja)
      
        cur.execute(qry)
        connection.commit()
     
def add_acc_distance():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS acc_distance numeric[]"
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
    qry="select ra_keys from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        ra_keys=each[0]
        acc_distance=0
        list_acc_distance=[0]
        i=0
        while i<len(ra_keys):
            k=ra_keys[i]
            distance=round(dict_ra_key[k],1)
            acc_distance+=distance
            # distance=str(round(dict_ra_key[k],1))
            list_acc_distance.append(str(acc_distance))
            i+=1
     
        qry="update "+tb_to_mongodb_railway_line+" set acc_distance=array"+str(list_acc_distance).replace("'","")+" where ra_keys=array"+str(ra_keys)

        cur.execute(qry)
        connection.commit()
    
    
def add_service_route():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS serviceroute_ja text[], ADD COLUMN IF NOT EXISTS serviceroute_en text[],ADD COLUMN IF NOT EXISTS serviceroute_zh text[]"
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
       
    qry="select points_ja from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()

    for each in r:  
        points_ja=each[0]
        list_service_route_ja=[]
        list_service_route_en=[]
        list_service_route_zh=[]
        for point in points_ja:
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
        qry="update "+tb_to_mongodb_railway_line+" set serviceroute_ja=array"+str(list_service_route_ja)+", serviceroute_en=array"+str(list_service_route_en)+", serviceroute_zh=array"+str(list_service_route_zh)+" where points_ja=array"+str(points_ja)
        cur.execute(qry)
        connection.commit()
                
def add_direct_service_route():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS direct_serviceroute_ja text[], ADD COLUMN IF NOT EXISTS direct_serviceroute_en text[],ADD COLUMN IF NOT EXISTS direct_serviceroute_zh text[]"
    cur.execute(qry)
    connection.commit()
    
    dict_ra_key={}
    dict_service_route={}
    qry="select ra_keys, service_route_ja, service_route_en, service_route_zh from "+tb_service_route +" order by array_length (stations, 1) desc"
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        ra_keys=each[0]
        service_route_ja=each[1].split("(")[0]
        service_route_en=each[2].split("(")[0]
        service_route_zh=each[3].split("(")[0]
        for ra_key in ra_keys:
            if ra_key not in dict_ra_key:
                dict_ra_key[ra_key]=[]
            dict_ra_key[ra_key].append(service_route_ja)
        if service_route_ja not in dict_service_route:
            dict_service_route[service_route_ja]=[service_route_en,service_route_zh]
            
    qry="select ra_keys from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()

    for each in r:  
        ra_keys=each[0]
        unsorted_serviceroute_ja=[]
        list_service_route_ja=[]
        list_service_route_en=[]
        list_service_route_zh=[]
        for ra_key in ra_keys:
            if ra_key in dict_ra_key:
                for l in dict_ra_key[ra_key]:
                    if l not in unsorted_serviceroute_ja:
                        unsorted_serviceroute_ja.append(l)
            
        for line in  unsorted_serviceroute_ja:
            if '新幹線' in line:
                list_service_route_ja.append(line)
        for line in unsorted_serviceroute_ja:
            if '新幹線' not in line and 'JR' in line:
                list_service_route_ja.append(line)
        for line in unsorted_serviceroute_ja:
            if 'JR' not in line and line not in list_service_route_ja:
                list_service_route_ja.append(line)
                left_2_word=line[:2]
                for l in unsorted_serviceroute_ja:
                    if 'JR' not in l and l not in list_service_route_ja and l[:2]==left_2_word:
                        list_service_route_ja.append(l)

        for line in list_service_route_ja:
            list_service_route_en.append(dict_service_route[line][0])
            list_service_route_zh.append(dict_service_route[line][1])
        if len(list_service_route_en)>0:
            qry="update "+tb_to_mongodb_railway_line+" set direct_serviceroute_ja=array"+str(list_service_route_ja)+", direct_serviceroute_en=array"+str(list_service_route_en)+", direct_serviceroute_zh=array"+str(list_service_route_zh)+" where ra_keys=array"+str(ra_keys)
            cur.execute(qry)
            connection.commit()
        else:
            qry="update "+tb_to_mongodb_railway_line+" set direct_serviceroute_ja='{}', direct_serviceroute_en='{}', direct_serviceroute_zh='{}' where ra_keys=array"+str(ra_keys)
            cur.execute(qry)
            connection.commit()

def add_way_line():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS way_line geometry"
    cur.execute(qry)
    connection.commit()
    qry="select ra_keys from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        qry="update "+tb_to_mongodb_railway_line+" set way_line=(select ST_LineMerge(ST_Union(c.way)) from (select way from "+tb_line+" a where a.ra_key =ANY ((select ra_keys from "+tb_to_mongodb_railway_line+" b where b.ra_keys = array"+str(ra_keys)+")::text[]) ) as c) where ra_keys=array"+str(ra_keys)
       
        cur.execute(qry)
        connection.commit()
        
def add_way_point():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS way_point geometry"
    cur.execute(qry)
    connection.commit()
    qry="select ra_keys, railway_line_ja from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        qry="update "+tb_to_mongodb_railway_line+" set way_point=(select  ST_Union(ST_Centroid(c.WAY)) from (select st_intersection(a.way, b.way_line) as way from "+tb_polygon+" a, "+tb_to_mongodb_railway_line+" b where a.name_ja =ANY ((select points_ja from "+tb_to_mongodb_railway_line+" b where b.ra_keys=array"+str(ra_keys)+")::text[]) and st_intersects(a.way, b.way_line) and b.ra_keys=array"+str(ra_keys)+") as c) where ra_keys=array"+str(ra_keys)
    
        cur.execute(qry)
        connection.commit()
def add_way_bounds():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS way_bounds geometry"
    cur.execute(qry)
    connection.commit()
    qry="select ra_keys from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        ra_keys=each[0]
        qry="update "+tb_to_mongodb_railway_line+" set way_bounds=(select ST_Envelope(ST_LineMerge(ST_Union(c.way))) from (select way from "+tb_line+" a where a.ra_key =ANY ((select ra_keys from "+tb_to_mongodb_railway_line+" b where b.ra_keys = array"+str(ra_keys)+")::text[]) ) as c) where ra_keys=array"+str(ra_keys)
       
        cur.execute(qry)
        connection.commit()

def add_catalog():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS catalog_ja text[], ADD COLUMN IF NOT EXISTS catalog_en text[], ADD COLUMN IF NOT EXISTS catalog_zh text[]"
    cur.execute(qry)
    connection.commit()
    dict_prefecture={}
    translate_catalog={}
    qry="select prefecture_ja, area_ja, area_en, area_zh from ctl_prefecture"
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        prefecture_ja=each[0]
        area_ja=each[1]
        area_en=each[2]
        area_zh=each[3]
        dict_prefecture[prefecture_ja]=area_ja
        if area_ja not in translate_catalog:
            translate_catalog[area_ja]=[area_en,area_zh]
    dict_point={}
    qry="select name_ja, prefecture from "+tb_polygon
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        name_ja=each[0]
        prefecture_ja=each[1]
        dict_point[name_ja]=dict_prefecture[prefecture_ja]
    qry="select points_ja, railway_line_ja,company_ja, company_en, company_zh from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r: 
        points_ja=each[0]
        railway_line_ja=each[1]
        company_ja=each[2]
        company_en=each[3]
        company_zh=each[4]
        list_area_ja=[]
        list_area_en=[]
        list_area_zh=[]
        if company_ja[:2]=='JR':
            list_area_ja.append(company_ja)
            list_area_en.append(company_en)
            list_area_zh.append(company_zh)
        else:
            for point in points_ja:
                area=dict_point[point]
                if area not in list_area_ja:
                    list_area_ja.append(area)
                    list_area_en.append(translate_catalog[area][0])
                    list_area_zh.append(translate_catalog[area][1])
        qry="update "+tb_to_mongodb_railway_line+" set catalog_ja=array"+str(list_area_ja)+", catalog_en=array"+str(list_area_en)+", catalog_zh=array"+str(list_area_zh)+" where railway_line_ja='"+railway_line_ja+"' and points_ja=array"+str(points_ja)
        cur.execute(qry)
        connection.commit()
                    
            
def add_total_priority():
    qry="ALTER TABLE "+tb_to_mongodb_railway_line  +" ADD COLUMN IF NOT EXISTS total_priority integer"
    cur.execute(qry)
    connection.commit()
    qry="select railway_line_ja, type, acc_distance from "+tb_to_mongodb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    dict_railwayline={}
    for each in r: 
        railway_line_ja=each[0]
        the_type=each[1]
        accdistance=each[2][-1]
        if the_type=='shinkansen':
            cal_distanct=int(1000*accdistance)
        elif the_type=='rail_JR':
            cal_distanct=int(100*accdistance)
        elif the_type=='rail_private':
            cal_distanct=int(10*accdistance)
        else:
            cal_distanct=int(accdistance)
        if railway_line_ja not in dict_railwayline:
            dict_railwayline[railway_line_ja]=0
        dict_railwayline[railway_line_ja]+=cal_distanct
    for railway_line_ja in dict_railwayline:
        qry="update "+tb_to_mongodb_railway_line+" set total_priority="+str(dict_railwayline[railway_line_ja])+" where railway_line_ja='"+railway_line_ja+"'"
        cur.execute(qry)
        connection.commit()           


def gen_mongo_railwayline_json():
    qry = "SELECT json_build_object('railwayline_ja',railway_line_ja, 'railwayline_en',railway_line_en,'railwayline_zh',railway_line_zh, 'company_ja', company_ja, 'company_en', company_en, 'company_zh', company_zh,  'official_name_ja', official_name_ja, 'official_name_en', official_name_en, 'official_name_zh', official_name_zh, 'points_ja', points_ja, 'points_en', points_en, 'points_zh', points_zh, 'prefecture_ja', prefecture_ja, 'prefecture_en', prefecture_en, 'prefecture_zh', prefecture_zh, 'type', type, 'ra_keys', ra_keys, 'priority', priority,'serviceroute_ja', serviceroute_ja, 'serviceroute_en', serviceroute_en, 'serviceroute_zh', serviceroute_zh, 'acc_distance', acc_distance, 'direct_serviceroute_ja', direct_serviceroute_ja, 'direct_serviceroute_en', direct_serviceroute_en, 'direct_serviceroute_zh', direct_serviceroute_zh, 'catalog_ja', catalog_ja, 'catalog_en', catalog_en, 'catalog_zh', catalog_zh, 'total_priority', total_priority, 'way_bounds',ST_AsGeoJSON(geog, 5)::jsonb) from (select * ,ST_Transform( way_BOUNDS, 4326) as geog from "+tb_to_mongodb_railway_line+") row"
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
    
    subprocess.call ('mongoimport --uri "mongodb+srv://'+mongodb_user+':'+mongodb_password+'@cluster0.jqnr5.mongodb.net/'+mongodb_database+'?retryWrites=true&w=majority"  --collection railwayline_'+config['version_code']+' --drop --file ' +to_file_path+'/'+to_file_name+' --jsonArray --maintainInsertionOrder --batchSize 1', shell=True )
    
create_new_to_mongodb_railway_line_table()
complete_company_points_prefecture()
add_acc_distance()
add_service_route()
add_direct_service_route()
add_way_line()
add_way_point()
add_way_bounds()
add_catalog()
add_total_priority()
gen_mongo_railwayline_json()