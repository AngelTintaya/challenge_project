import mysql.connector
mydb = mysql.connector.connect(
    host="localhost",
    port="23306",
    user="userdb",
    passwd="userpwd",
    database="db_company"
)

mycursor = mydb.cursor()

# Creating table departments
mycursor.execute("DROP TABLE IF EXISTS departments")
mycursor.execute("""CREATE TABLE departments (
                    id INTEGER,
                    department VARCHAR(255),
                    PRIMARY KEY (id)
                    )""")

# Creating table jobs
mycursor.execute("DROP TABLE IF EXISTS jobs")
mycursor.execute("""CREATE TABLE jobs (
                    id INTEGER,
                    job VARCHAR(255),
                    PRIMARY KEY (id)
                    )""")

# Creating table jobs
mycursor.execute("""DROP TABLE IF EXISTS hired_employees""")
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

print('Company Model was successfully created!')