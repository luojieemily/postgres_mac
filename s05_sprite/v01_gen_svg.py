import configparser
import psycopg2
from configparser import ConfigParser
import os
import json
f = open("colors.json", "r")
dict_color = json.load(f)
print(dict_color)


config_parser = ConfigParser()
config_parser.read('../config.cfg')
config = config_parser['DEFAULT']
database = config['database']+'_'+config['version_code']
hostname = config['hostname']
username = config['username']
password = config['password']
tb_to_mongodb_train = 'to_mongodb_train_'+config['version_code']
icon_width = 500


connection = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database)
cur = connection.cursor()

qry = "select distinct(icon_image) from " + tb_to_mongodb_train
print(qry)
cur.execute(qry)
r = cur.fetchall()
for each in r:
    icon_image = each[0]
    backgroundColor = dict_color[icon_image][0]
    wordColor = dict_color[icon_image][2]
    direction = ['', '-b']
    for d in direction:
        fileName = icon_image+d
        if d == "-b":
            backgroundColor = dict_color[icon_image][1]
        with open(os.getcwd()+'/svg/'+fileName+'.svg', 'w') as f:
            f.write('<svg width="'+str(icon_width)+'" height="' +
                    str(icon_width)+'" xmlns="http://www.w3.org/2000/svg"><filter id="glow" x="-15%" y="-15%" width="130%" height="130%"><feGaussianBlur stdDeviation="5 5" result="glow"/><feMerge><feMergeNode in="glow"/><feMergeNode in="glow"/><feMergeNode in="glow"/></feMerge></filter>')
            f.write(' <g id="Layer_1">')
            f.write('<title>Layer 1</title>')
            f.write('<ellipse stroke-width="0" ry="'+str(icon_width/2-icon_width/20)+'" rx="'+str(icon_width/2-icon_width /
                    20)+'" id="svg_2" cy="'+str(icon_width/2)+'" cx="'+str(icon_width/2)+'" fill="'+backgroundColor+'" />')
            f.write('<text style="filter: url(#glow); fill: #3a3a38" font-size="'+str(icon_width/1.78)+'"  font-weight="bold" x="'+str(icon_width/2) +
                    '" y="'+str(icon_width/1.9)+'" text-anchor="middle"  fill="white" dy="'+str(icon_width/6)+'" >'+icon_image+'</text>')
            f.write('<text  font-size="'+str(icon_width/1.8)+'"   font-weight="bold" x="'+str(icon_width/2) +
                    '" y="'+str(icon_width/1.9)+'" text-anchor="middle"  fill="white" dy="'+str(icon_width/6)+'" >'+icon_image+'</text>')

            f.write(' </g>')
            f.write('</svg>')
