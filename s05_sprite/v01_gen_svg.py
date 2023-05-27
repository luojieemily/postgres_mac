import configparser
import psycopg2
from configparser import ConfigParser
import os
import json
f = open("colors.json", "r")
dict_color= json.load(f)
# print(f.read())
print(dict_color)


config_parser=ConfigParser()
config_parser.read('../config.cfg')
config=config_parser['DEFAULT']
database=config['database']+'_'+config['version_code']
hostname=config['hostname']
username=config['username']
password=config['password']
tb_to_mongodb_train='to_mongodb_train_'+config['version_code']
icon_width=500
# dict_color={"color_shinkansen_outter":'#d1c4e9',"color_shinkansen_inner":'#4527a0',"color_rail_jr_outter":'#c8e6c9',"color_rail_jr_inner":'#2e7d32',"color_rail_private_outter":'#b2dfdb',"color_rail_private_inner":'#00695c',"color_subway_outter":'#ffcdd2',"color_subway_inner":'#c62828',"color_tram_outter":'#ffccbc',"color_tram_inner":'#d84315',"color_others_outter":'#c5cae9',"color_others_inner":'#283593'}


connection=psycopg2.connect(host=hostname, user=username, password=password, dbname=database)
cur=connection.cursor()

qry="select distinct(icon_image) from " +tb_to_mongodb_train
print(qry)
cur.execute(qry)
r=cur.fetchall()
for each in r: 
    print(each)
    icon_image=each[0]
    train_type=icon_image[:].split("-")[0]
    word=icon_image[:].split("-")[1]
    # outer_color=dict_color['color_'+train_type+"_outter"]
    # inner_color=dict_color['color_'+train_type+"_inner"]
    outer_color=dict_color[ icon_image]
    inner_color=dict_color[ icon_image]
    
    with open(os.getcwd()+'/svg/'+icon_image+'.svg', 'w') as f:
         f.write('<svg width="'+str(icon_width)+'" height="'+str(icon_width)+'" xmlns="http://www.w3.org/2000/svg">')
         f.write(' <g id="Layer_1">')
         f.write('<title>Layer 1</title>')
        #  f.write('<ellipse stroke-width="0" ry="'+str(icon_width/2)+'" rx="'+str(icon_width/2)+'" id="svg_1" cy="'+str(icon_width/2)+'" cx="'+str(icon_width/2)+'" fill="'+outer_color+'" />')
         f.write('<ellipse stroke-width="0" ry="'+str(icon_width/2-icon_width/20)+'" rx="'+str(icon_width/2-icon_width/20)+'" id="svg_2" cy="'+str(icon_width/2)+'" cx="'+str(icon_width/2)+'" fill="'+inner_color+'" />')
         f.write('<text font-size="'+str(icon_width/2)+'"  font-style="normal" font-weight="bold" x="'+str(icon_width/2)+'" y="'+str(icon_width/2)+'" text-anchor="middle"  fill="white" dy="'+str(icon_width/6)+'" >'+word+'</text>')
         f.write(' </g>')
         f.write('</svg>')


    
   