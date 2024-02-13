import psycopg2
from configparser import ConfigParser
import pymongo
from r01_prefecture import r01_prefecture
from r02_company import r02_company
from r03_railwayline import r03_railwayline
config_parser = ConfigParser()
config_parser.read('../config.cfg')
config = config_parser['RAILAROUND']
mg_user = config['mg_user']
mg_password = config['mg_password']
mg_database = config['mg_database']
myclient = pymongo.MongoClient('mongodb+srv://'+mg_user+':'+mg_password +
                               '@cluster0.jqnr5.mongodb.net/'+mg_database+'?retryWrites=true&w=majority')
mgdb = myclient[mg_database]


with myclient.start_session() as session:
    with session.start_transaction():
        # r01_prefecture(session, mgdb)
        # r02_company(session, mgdb)
        r03_railwayline(session, mgdb)


# Upon normal completion of with session.start_transaction() block, the transaction automatically calls ClientSession.commit_transaction(). If the block exits with an exception, the transaction automatically calls ClientSession.abort_transaction().
