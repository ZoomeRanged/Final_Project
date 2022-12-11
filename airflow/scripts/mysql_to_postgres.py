
from pyspark.sql import SparkSession
import findspark
import pyspark
findspark.init()



CONNECTOR_TYPE = 'com.mysql.jdbc.Driver'
SQL_USERNAME = 'root'
SQL_PASSWORD = 'admin'
SQL_DBNAME = 'Final_Project'
SQL_SERVERNAME = 'localhost'

url = "jdbc:mysql://host.docker.internal:3306/Final_Project"

table_name1 = "home_credit_default_risk_application_test"
table_name2 = "home_credit_default_risk_application_train"

spark = SparkSession.builder.config('spark.jars', '/opt/bitnami/spark/jars/mysql-connector-j-8.0.31.jar').appName('app').getOrCreate()


df = spark.read\
        .format('jdbc') \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name1) \
        .option("user", SQL_USERNAME) \
        .option("password", SQL_PASSWORD) \
        .load()
        
df1 = spark.read\
        .format('jdbc') \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name2) \
        .option("user", SQL_USERNAME) \
        .option("password", SQL_PASSWORD) \
        .load()



 
#write to postgres
CONNECTOR_TYPE = 'org.postgresql.Driver'
PSQL_USERNAME = 'postgres'
PSQL_PASSWORD = 'admin'
PSQL_DBNAME = 'postgres'
PSQL_SERVERNAME = 'localhost'
PSQL_Port = '5432'

url_psql = "jdbc:postgresql://host.docker.internal:3306/postgres"
spark = SparkSession.builder.config('spark.jars', '/opt/bitnami/spark/jars/mysql-connector-j-8.0.31.jar').appName('app').getOrCreate()

table1 = 'Application_Test'
table2 = 'Apllication_train'


        
df.write\
        .format('jdbc') \
        .option('driver', CONNECTOR_TYPE) \
        .option("url", url_psql) \
        .option("dbtable", table1) \
        .option("user", PSQL_USERNAME) \
        .option("password", PSQL_PASSWORD) \
        .save()
       
df1.write\
        .format('jdbc') \
        .option('driver', CONNECTOR_TYPE) \
        .option("url", url_psql) \
        .option("dbtable", table2) \
        .option("user", PSQL_USERNAME) \
        .option("password", PSQL_PASSWORD) \
        .save()

