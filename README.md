# apache-airflow
Playing with Apache Airflow, Python, MySQL, PostgreSQL, MongoDB

Environment setup:  
- Using WSL2 on Windows
- Ubuntu 22.04.3 LTS

- Install VS Code  
Get started using Visual Studio Code with Windows Subsystem for Linux  
https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-vscode  

Get started with databases on Windows Subsystem for Linux  
https://learn.microsoft.com/en-us/windows/wsl/tutorials/wsl-database  

- Install MySQL
$ sudo apt update  
$ sudo apt install mysql-server  
$ mysql --version  
$ systemctl status mysql  
$ sudo service mysql status  
$ sudo mysql  

- Setting up a MySQL Database  
CREATE DATABASE airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;  
CREATE USER 'airflow_user' IDENTIFIED BY 'airflow_pass';  
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user';  

- Install PostgreSQL
$ sudo apt update  
$ sudo apt install postgresql postgresql-contrib  
$ psql --version  

- Operational commands
$ sudo service postgresql status  
$ sudo service postgresql start  
$ sudo service postgresql restart  
$ sudo service postgresql stop  
  
$ sudo -i -u postgres  
~$ psql  
  
- listing databases available
postgres=# \l+  
  
- connect to a database
\connect DBNAME  
\c DBNAME  
  
- Setting up a PostgreSQL Database
CREATE DATABASE airflow_db;  
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';  
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;  
-- PostgreSQL 15 requires additional privileges:  
USE airflow_db;  
GRANT ALL ON SCHEMA public TO airflow_user;  
  
postgres-# \connect airflow_db  
You are now connected to database "airflow_db" as user "postgres".  
airflow_db-#  
  
- list all the tables in the current schema
\dt  
  
airflow_db-# \dt  

- Set up a Database Backend
https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html  
  
- Install MongoDB
Install MongoDB Community Edition on Ubuntu  
https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-ubuntu/#std-label-install-mdb-community-ubuntu  
  
$ cat /etc/lsb-release
DISTRIB_ID=Ubuntu  
DISTRIB_RELEASE=22.04  
DISTRIB_CODENAME=jammy  
DISTRIB_DESCRIPTION="Ubuntu 22.04.3 LTS"  
  
- Check if mongodb is installed on your system  
$ mongod --version  
  
$ sudo apt-get update  
$ sudo apt-get install gnupg curl  
  
$ curl -fsSL https://www.mongodb.org/static/pgp/server-7.0.asc | \  
   sudo gpg -o /usr/share/keyrings/mongodb-server-7.0.gpg \  
   --dearmor  
  
$ echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/7.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-7.0.list  
  
$ sudo apt-get update  
$ sudo apt-get install -y mongodb-org  
$ mongod --version  
  
$ ps --no-headers -o comm 1  
systemd  
  
- Start MongoDB
You can start the mongod process by issuing the following command:  
$ sudo systemctl start mongod  

- Verify that MongoDB has started successfully.
$ sudo systemctl status mongod  

- MongoDB Shell
https://www.mongodb.com/docs/mongodb-shell/#mongodb-binary-bin.mongosh  
Begin using MongoDB.  
$ mongosh  
  
- This is equivalent to the following command:
$ mongosh "mongodb://localhost:27017"  
  
- Install Apache Airflow
Quick Start  
https://airflow.apache.org/docs/apache-airflow/stable/start.html  
  
$ export AIRFLOW_HOME=~/airflow  
$ source ~/.bashrc  
  
$ AIRFLOW_VERSION=2.9.1  
$ PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"  
$ PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"  
$ CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"  
  
$ sudo apt update  
$ sudo apt install python3-pip  
  
$ pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"  
  
- Run Airflow Standalone:
$ airflow standalone  
OR  
$ airflow webserver --port 8080  
$ airflow scheduler  
  
- Browse to: localhost:8080
Login with username: admin  password: xxxxx  

- Create Connections to the database in Airflow:
  - mysql_default  
  - postgres_default  
  - mondo_default  
  
- Install Airflow providers
$ pip install apache-airflow-providers-postgres  
$ pip install apache-airflow-providers-mysql  
$ pip install apache-airflow-providers-mongo  
  
- Installing pandas
https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html  
$ pip install pandas  

- DAG scripts:  
- src/cross_database_employee_reporting_dag.py  
- src/mongodb_ops_employee_dag.py  
- src/mysql_hr_employee_dag.py  
- src/postgres_it_employee_dag.py  

