'''
调用terminal,使用tippecanoe生成mbtiles文件
'''

import subprocess
from configparser import ConfigParser
config_parser = ConfigParser()
config_parser.read('../config.cfg')
config = config_parser['DEFAULT']
geojson_path = config['GEOJSON_DIRECTORY']
mbtiles_path = config['MBTILES_DIRECTORY']


types = ['polygon', 'point', 'line']
# types=['point','line']
layer_data = ''
# -L parcels:parcels.geojson -L buildings:buildings.geojson
for t in types:
    geojson_file_name = geojson_path+'/'+t + \
        '_'+config['version_code']+'.geojson'
    layer_data += ' -L '+t+":"+geojson_file_name

mbtiles_file_name = mbtiles_path+'/'+'jp_data' + \
    '_'+config['version_code']+'.mbtiles'

# subprocess.call('tippecanoe -o '+mbtiles_file_name+' -z 16 --force '+ geojson_file_name, shell=True, cwd=mbtiles_path)
subprocess.call('tippecanoe -o '+mbtiles_file_name +
                ' -z16 -r1 --force ' + layer_data, shell=True, cwd=mbtiles_path)

# -r1: If you have a smaller data set where all the points would fit without dropping any of them, use -r1 to keep them all.

# -z3: Only generate zoom levels 0 through 3
