import sqlalchemy as db
import configparser
import mysql.connector

# Getting secrets
config = configparser.ConfigParser()
config.read('config.ini')
db_pass = config['api']['passwd']


def generate_engine():

    # Connection data
    host = 'localhost'
    port = '23306'
    user = 'userdb'
    passwd = db_pass
    database = 'db_company'

    # Engine from sqlalchemy
    engine = db.create_engine(f'mysql+mysqlconnector://{user}:{db_pass}@{host}:{port}/{database}', echo=False)

    return engine
