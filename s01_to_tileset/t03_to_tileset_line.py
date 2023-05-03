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
tb_to_tileset_line='to_tileset_line_'+config['version_code']
tb_line='line'
tb_railway_line='railway_line'
tb_service_route='service_route'

connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()


def create_new_line_table():
    qry="DROP TABLE IF EXISTS "+tb_to_tileset_line
    cur.execute(qry)
    connection.commit()

    qry="CREATE TABLE "+tb_to_tileset_line+" as select way, ra_key, railway_line as railway_line_ja from "+tb_line
    cur.execute(qry)
    connection.commit()  

def add_railway_line():
    qry="alter table "+tb_to_tileset_line+" add column railway_line_en text, add column railway_line_zh text"
    cur.execute(qry)
    connection.commit()
    qry="select railway_line_ja, ra_keys from "+tb_railway_line 
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        railway_line=each[0]
        ra_keys=each[1]
       
        for l in ra_keys:
            qry="update "+tb_to_tileset_line+" set railway_line_ja='"+railway_line+"' where ra_key='"+l+"'"
            cur.execute(qry)
            connection.commit()
            
def add_en_zh_for_railway_line():

    qry="select railway_line_ja, railway_line_en, railway_line_zh from "+tb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        railway_line_ja=each[0]
        railway_line_en=each[1]
        railway_line_cn=each[2]
        qry="update "+tb_to_tileset_line+" set railway_line_en='"+railway_line_en+"', railway_line_zh='"+railway_line_cn+"' where railway_line_ja='"+railway_line_ja+"'"
        cur.execute(qry)
        connection.commit()

def add_type():
    qry="alter table "+tb_to_tileset_line+" add column type_shinkansen integer, add column type_rail_jr integer, add column type_rail_private integer, add column type_subway integer, add column type_tram integer, add column type_others integer"
    cur.execute(qry)
    connection.commit()
    dict_type={}  #dict[shinkansen]=['东京','新横滨','新大阪']
    dict_type['shinkansen']=[]
    dict_type['rail_jr']=[]
    dict_type['rail_private']=[]
    dict_type['subway']=[]
    dict_type['tram']=[]
    dict_type['others']=[]
    
    qry="select type, ra_keys from "+tb_railway_line
    cur.execute(qry)
    r=cur.fetchall()
    for each in r:
        if 'shinkansen'==each[0]:
            dict_type['shinkansen']+=each[1]
        elif 'rail_JR'==each[0]:
            dict_type['rail_jr']+=each[1]
        elif 'rail_private'==each[0]:
            dict_type['rail_private']+=each[1]
        elif 'subway'==each[0]:
            dict_type['subway'] +=each[1]
        elif 'tram'==each[0]:
            dict_type['tram']+=each[1]
        else:
            dict_type['others']+=each[1]
    for the_type in ['shinkansen','rail_jr','rail_private','subway','tram','others']:
        ra_keys=list(set(dict_type[the_type]))
        type_col='type_'+the_type
        for ra_key in ra_keys:
            qry="update "+tb_to_tileset_line+" set "+type_col+" = 1 where ra_key='"+ra_key+"'"
            cur.execute(qry)
            connection.commit()
        qry="select railway_line_ja from "+tb_to_tileset_line+" where "+type_col+" =1"
        cur.execute(qry)
        r=cur.fetchall()
        l=[]
        for each in r:
            railway_line_ja=each[0]
            if railway_line_ja not in l:
                qry="update " +tb_to_tileset_line+" set "+type_col +"=1 where railway_line_ja='"+railway_line_ja+"'"
                cur.execute(qry)
                connection.commit()
                l.append(railway_line_ja)
            
        qry="update "+tb_to_tileset_line+" set "+type_col+" = 0 where "+ type_col+" is null"
        cur.execute(qry)
        connection.commit()

        
       
create_new_line_table()
add_railway_line()    
add_en_zh_for_railway_line()
add_type()

