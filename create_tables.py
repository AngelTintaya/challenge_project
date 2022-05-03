import mysql.connector
import configparser

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


def main():
    pass

def clean_tables():
    # Dropping Tables
    mycursor.execute("DROP TABLE IF EXISTS hired_employees")
    mycursor.execute("DROP TABLE IF EXISTS departments")
    mycursor.execute("DROP TABLE IF EXISTS jobs")

    mycursor.execute("DROP TABLE IF EXISTS failed_hired_employees")
    mycursor.execute("DROP TABLE IF EXISTS failed_departments")
    mycursor.execute("DROP TABLE IF EXISTS failed_jobs")

    # Creating table departments
    mycursor.execute("""CREATE TABLE departments (
                        id INTEGER,
                        department VARCHAR(255),
                        PRIMARY KEY (id)
                        )""")

    mycursor.execute("""CREATE TABLE failed_departments (
                        id INTEGER,
                        department VARCHAR(255)
                        )""")

    # Creating table jobs
    mycursor.execute("""CREATE TABLE jobs (
                        id INTEGER,
                        job VARCHAR(255),
                        PRIMARY KEY (id)
                        )""")

    mycursor.execute("""CREATE TABLE failed_jobs (
                        id INTEGER,
                        job VARCHAR(255)
                        )""")

    # Creating table jobs
    mycursor.execute("""CREATE TABLE hired_employees (
                        id INTEGER,
                        name VARCHAR(255),
                        datetime VARCHAR(255),
                        department_id INTEGER,
                        job_id INTEGER,
                        PRIMARY KEY (id),
                        FOREIGN KEY (department_id) REFERENCES departments(id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id)
                        )""")

    mycursor.execute("""CREATE TABLE failed_hired_employees (
                        id INTEGER,
                        name VARCHAR(255),
                        datetime VARCHAR(255),
                        department_id VARCHAR(255),
                        job_id VARCHAR(255)
                        )""")

    print('Company Model was successfully created!')


if __name__ == "__main__":
    clean_tables()
