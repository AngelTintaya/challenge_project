import pandas as pd
import configparser
import mysql.connector

# Getting secrets
config = configparser.ConfigParser()
config.read('config.ini')
db_pass = config['api']['passwd']

# Connecting to the database
mydb = mysql.connector.connect(
    host="localhost",
    port="23306",
    user="userdb",
    passwd=db_pass,
    database="db_company"
)
mycursor = mydb.cursor()
chunksize = 10  # 1000

def parseCSV(filePath):
    col_names = ['id', 'job']
    # df_data = pd.read_csv(filePath, names=col_names, header=None)
    for df_chunk_data in pd.read_csv(filePath, names=col_names, header=None, chunksize=chunksize):
        batch_rows = [(str(row["id"]), row["job"]) for i, row in df_chunk_data.iterrows()]
        sql = "INSERT INTO jobs (id, job) VALUES (%s, %s)"
        mycursor.executemany(sql, batch_rows)
        mydb.commit()

parseCSV('static/files/jobs.csv')
