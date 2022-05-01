# challenge_project
This is a project to creare an API REST to load CSV data into a Database

- To execute docker compose:
docker-compose up -d
- Verificar que se haya levantado el docker-compose:
docker-compose ps

Installing MySQL Library in Mac M1:

- Execute the command (Need to have homebrew)
arch -arm64 brew install mysql-client
- Put mysql-client in path:
export PATH="/usr/local/opt/openssl/bin:$PATH"
(Then re-open terminal)
- Set flags in terminal
export LDFLAGS="-L/usr/local/opt/openssl/lib"
export CPPFLAGS="-I/usr/local/opt/openssl/include"
- Execute:
pip install mysqlclient
pip install mysql
pip install mysql-connector
