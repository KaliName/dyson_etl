# Airflow DAG development with tests

# # Setup
docker-compose up

New terminal:
```javascript
docker container list
```
```javascript
docker exec -it 
```
```javascript
docker exec -it dockerContainerID sh
```
```javascript
pip install pymongo
pip install sqlalchemy
pip install mysql-connector
```

# # Links
MongoExpress
http://localhost:8081/

Airflow
http://localhost:8080/

MySql
http://localhost:8082/

Connection with Adminer and MySQL
Go back in Adminer (http://localhost:8082/). Then complete the form:

```javascript
“System” : select “MySQL” ;
“Server” : type “dbMysql” ;
“Username” : type “root” ;
“Password” : type “pass” ;
“Database” : type “dyson_etl” ;
```

Click “login” button.