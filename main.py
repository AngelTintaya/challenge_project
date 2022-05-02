from flask import Flask, render_template, request, redirect, url_for
import os
import configparser
import pandas as pd
import mysql.connector
from werkzeug.utils import secure_filename
from sqlalchemy import create_engine


chunksize = 1000
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
engine = create_engine(f'mysql+mysqlconnector://{user}:{db_pass}@{host}:{port}/{database}', echo=False)
'''
# Connecting to the database
mydb = mysql.connector.connect(
    host="localhost",
    port="23306",
    user="userdb",
    passwd=db_pass,
    database="db_company"
)
mycursor = mydb.cursor()
'''

app = Flask(__name__)
app.config["DEBUG"] = True  # Enabling debugging mode
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024  # Limiting the size of uploading files (1mb)
app.config['UPLOAD_EXTENSIONS'] = ['.csv']  # Validating filenames

# Upload folder
UPLOAD_FOLDER = 'static/files'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route("/")
# Root URL
def index():
    return render_template('index.html')


@app.route("/", methods=['POST'])
def upload_files():
    # get the uploaded file
    uploaded_file = request.files['file']
    file_name = secure_filename(uploaded_file.filename)  # Validating filenames
    if file_name != '':
        file_root, file_ext = os.path.splitext(file_name)
        if file_root not in ['jobs', 'departments', 'hired_employees']:
            return {'message': 'File name is not part of data model'}
        elif file_ext not in app.config['UPLOAD_EXTENSIONS']:  # Handling extentions
            return {'message': 'File does not have correct extension'}
        else:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
            # Set the file path
            uploaded_file.save(file_path)

            # Loading data to database
            parse_csv(file_path)
        # save the file
    return redirect(url_for('index'))


def parse_csv(filepath):
    file_name = filepath.split('/')[-1].split('.')[0]

    if file_name == 'jobs':
        col_names = ['id', 'job']

        # df_data = pd.read_csv(filepath, names=col_names, header=None)
        for df_chunk_data in pd.read_csv(filepath, names=col_names, header=None, chunksize=chunksize):
            df_chunk_data.to_sql(name='jobs', con=engine, if_exists='append', index=False)
            '''
            batch_rows = [(str(row["id"]), row["job"]) for i, row in df_chunk_data.iterrows()]
            sql = "INSERT INTO jobs (id, job) VALUES (%s, %s)"
            mycursor.executemany(sql, batch_rows)
            mydb.commit()
            '''

    elif file_name == 'departments':
        col_names = ['id', 'department']

        # df_data = pd.read_csv(filepath, names=col_names, header=None)
        for df_chunk_data in pd.read_csv(filepath, names=col_names, header=None, chunksize=chunksize):
            df_chunk_data.to_sql(name='departments', con=engine, if_exists='append', index=False)
            '''
            batch_rows = [(str(row["id"]), row["department"]) for i, row in df_chunk_data.iterrows()]
            sql = "INSERT INTO departments (id, department) VALUES (%s, %s)"
            mycursor.executemany(sql, batch_rows)
            mydb.commit()
            '''

    elif file_name == 'hired_employees':
        col_names = ['id', 'name', 'datetime', 'department_id', 'job_id']

        # df_data = pd.read_csv(filepath, names=col_names, header=None)
        for df_chunk_data in pd.read_csv(filepath, names=col_names, header=None, chunksize=chunksize):
            keys = ['id', 'department_id', 'job_id']
            df_correct = df_chunk_data[df_chunk_data[keys].notna().all(1)]
            df_correct = df_correct.fillna('')
            df_correct.to_sql(name='hired_employees', con=engine, if_exists='append', index=False)

            df_failed = df_chunk_data[~df_chunk_data[keys].notna().all(1)]
            df_failed = df_failed.fillna('')
            df_failed.to_sql(name='failed_hired_employees', con=engine, if_exists='append', index=False)

            #df_failed = df_chunk_data[df_chunk_data[keys].isna().any(1)]
            #df_correct = df_chunk_data[~df_chunk_data[keys].isna().any(1)]

            '''
            # Correct rows
            batch_rows = []
            for i, row in df_correct.iterrows():
                t_row = (str(row["id"]), row["name"], str(row["datetime"]), str(row["department_id"]), str(row["job_id"]))
                batch_rows.append(t_row)

            sql = "INSERT INTO hired_employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)"
            mycursor.executemany(sql, batch_rows)
            mydb.commit()

            # Failed rows
            failed_rows = []
            for i, row in df_failed.iterrows():
                t_row = (str(row["id"]), row["name"], str(row["datetime"]), str(row["department_id"]), str(row["job_id"]))
                failed_rows.append(t_row)

            sql = "INSERT INTO failed_hired_employees (id, name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)"
            mycursor.executemany(sql, failed_rows)
            mydb.commit()
            '''


if __name__ == "__main__":
    app.run(port=5000)
