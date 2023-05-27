'''
调用terminal,使用tippecanoe生成mbtiles文件
'''

import subprocess
from configparser import ConfigParser
config_parser=ConfigParser()
config_parser.read('../config.cfg')
config=config_parser['DEFAULT']
geojson_path=config['GEOJSON_DIRECTORY']
mbtiles_path=config['MBTILES_DIRECTORY']



layer_data=''
# -L parcels:parcels.geojson -L buildings:buildings.geojson

geojson_file_name=geojson_path+'/point_vip_'+config['version_code']+'.geojson'
layer_data+=' -L point:'+geojson_file_name
    
mbtiles_file_name=mbtiles_path+'/point_vip_'+config['version_code']+'.mbtiles'
            
# subprocess.call('tippecanoe -o '+mbtiles_file_name+' -z 16 --force '+ geojson_file_name, shell=True, cwd=mbtiles_path)
subprocess.call('tippecanoe -z6 -o '+mbtiles_file_name+' -r1  --force '+ layer_data, shell=True, cwd=mbtiles_path)
    
# -r1: If you have a smaller data set where all the points would fit without dropping any of them, use -r1 to keep them all. 