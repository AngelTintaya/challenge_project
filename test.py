import pandas as pd
import configparser
import mysql.connector
import sqlalchemy as db
import pandas as pd
import json
from json2html import json2html

# Getting secrets
config = configparser.ConfigParser()
config.read('config.ini')
db_pass = config['api']['passwd']

# Connection data
host = 'localhost'
port = '23306'
user = 'userdb'
passwd = db_pass
database = 'db_company'

# Engine from sqlalchemy
engine = db.create_engine(f'mysql+mysqlconnector://{user}:{db_pass}@{host}:{port}/{database}', echo=False)


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


def report_employee():
    sql = """
    SELECT
    d.department,
    j.job,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4
    FROM hired_employees he
    LEFT JOIN departments d on he.department_id = d.id
    LEFT JOIN jobs j on he.job_id = j.id
    WHERE 1 = 1
    AND YEAR(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) = 2021
    GROUP BY
    d.department,
    j.job
    ORDER BY d.department DESC, j.job desc
    """
    mycursor.execute(sql)
    myresult = mycursor.fetchall()

    for x in myresult:
        print(x)

    print('End of retrieval')

def report_employee_2():
    sql = """
    SELECT
    d.department,
    j.job,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
    SUM(CASE WHEN MONTH(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4
    FROM hired_employees he
    LEFT JOIN departments d on he.department_id = d.id
    LEFT JOIN jobs j on he.job_id = j.id
    WHERE 1 = 1
    AND YEAR(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) = 2021
    GROUP BY
    d.department,
    j.job
    ORDER BY d.department DESC, j.job desc
    """
    df = pd.read_sql(sql, con=engine)
    print(df[:5].to_dict('records'))  # List

def report_employee_3():
    sql = """
    SELECT
    AVG(dp.total)
    FROM (
            SELECT
            he.department_id,
            count(1) as total
            FROM hired_employees he
            WHERE 1=1
            AND YEAR(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) = 2021
            GROUP BY
            he.department_id
         ) dp
    """
    df = pd.read_sql(sql, con=engine)
    print(df.to_dict('records'))  # list

    print('End of retrieval')


def report_employee_4():
    sql = """
    SELECT
    he.department_id,
    count(1) as total
    FROM hired_employees he
    WHERE 1=1
    AND YEAR(STR_TO_DATE(he.datetime,'%Y-%m-%dT%TZ')) = 2021
    GROUP BY
    he.department_id
    """
    df = pd.read_sql(sql, con=engine)
    print(df.to_dict('records'))  # list


def report_employee_5():
    sql = """
    SELECT
    he.department_id as id,
    d.department,
    count(1) as hired
    FROM hired_employees he
    LEFT JOIN departments d on he.department_id = d.id
    GROUP BY
    he.department_id,
    d.department
    ORDER BY
    hired DESC
    """
    df = pd.read_sql(sql, con=engine)
    return df.to_dict('records')  # list


def split_dataframe(df, chunk_size=1000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

# parseCSV('static/files/jobs.csv')
# report_employee()
# report_employee_2()
# report_employee_3()
result = report_employee_5()

html_json = json2html.convert(json= result)
with open('./templates/report_1.html', 'w') as html_file:
    html_file.write(str(html_json))
    print('Json file converted successfully')
print(result)
print(html_json)

# split_dataframe(list(range(10)), 2)