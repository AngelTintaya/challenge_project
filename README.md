# challenge_project
This is a project to create an API REST to load CSV data into a Database.

Technologies used in the project:
- Flask
- MySQL
- Python
- Docker Container
- Git Flow

Directories:
- static/files: Contains data that were uploaded to the webpage.
- static/backups: Contains backup data
- templates: Contains HTML that will let us upload files.
- schemas: (Not loaded). Contains the database.
- venv: (Not loaded). Contains the virtual environment.

Files:
- create_tables.py: Will let us drop and create tables to the database.
- docker-compose.yaml: Will let us create a MySQL Database and have it running using containers.
- Dockerfile: Contains the image used for MySQL.
- main.py: Contains the script of the API Rest.
- requirements.txt: Contains all libraries that will be needed to execute the project.
- test.py: Script to test some features for the project.
- version.txt: To identify the current version of the project.
- backup_tables.py: Script to back up tables
- restore_tables: Script to restore tables
- config.py: Let us centralize the engine connection
