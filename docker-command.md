# init docker
## create docker network
``
docker network default_network
``

# airflow
``
docker-compose up -d --build
``

# spark
``
docker-compose up -d
``

# kafka
``
docker-compose up -d
``

# postgresql
``
docker run --name postgres-ds9 --network=default_network -p 5434:5432 -e POSTGRES_PASSWORD=admin -d postgres
``

# mysql
``
docker run --name mysql-ds9 --network=default_network -p 3306:3306 -e MYSQL_ROOT_PASSWORD=admin -d mysql
``