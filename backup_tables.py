import configparser
import pandas as pd
import sqlalchemy as db
from fastavro import writer, parse_schema
import datetime
import os

today = datetime.date.today()
root_path = './static/backups/'

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


def main():
    # Defining schemas
    schema_job, schema_department, schema_hired_employee = defining_schemas()

    # Saving avro files
    filename_job = save_avro('jobs', schema_job)
    filename_department = save_avro('departments', schema_department)
    filename_hired_employee = save_avro('hired_employees', schema_hired_employee)

    # Deleting deprecated files
    for file in os.listdir(root_path):
        if file not in [filename_job, filename_department, filename_hired_employee]:
            os.remove(f'{root_path}{file}')


def defining_schemas():
    job = {
        'doc': 'company jobs',
        'name': 'jobs',
        'namespace': 'db_company',
        'type': 'record',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'job', 'type': 'string'}
        ]
    }

    department = {
        'doc': 'company departments',
        'name': 'departments',
        'namespace': 'db_company',
        'type': 'record',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'department', 'type': 'string'}
        ]
    }

    hired_employee = {
        'doc': 'company hired employees',
        'name': 'hired_employee',
        'namespace': 'db_company',
        'type': 'record',
        'fields': [
            {'name': 'id', 'type': 'int'},
            {'name': 'name', 'type': 'string'},
            {'name': 'datetime', 'type': 'string'},
            {'name': 'department_id', 'type': 'int'},
            {'name': 'job_id', 'type': 'int'}
        ]
    }

    return job, department, hired_employee


def save_avro(table_name, schema):
    sql = f"SELECT * FROM {table_name}"
    df = pd.read_sql(sql, con=engine)

    if df.shape[0] != 0:
        # Converting all float results to int
        float_col = df.select_dtypes(include=['float64'])  # This will select float columns only
        for col in float_col.columns.values:
            df[col] = df[col].astype('int64')

        parsed_schema = parse_schema(schema)
        records = df.to_dict('records')

        file_path = f'{root_path}{table_name}_{today}.avro'
        with open(file_path, 'wb') as out:
            writer(out, parsed_schema, records)

        file_name = file_path.split('/')[-1]
    else:
        # Returning last file
        file_name = None
        for file in os.listdir(root_path):
            if table_name in file:
                file_name = file

    return file_name


if __name__ == '__main__':
    main()
