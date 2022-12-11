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

path1 = 'D:\\Final_Project\\spark\\resources\\application_test.csv'
path2 = 'D:\\Final_Project\\spark\\resources\\application_train.csv'

table_name1 = "home_credit_default_risk_application_test"
table_name2 = "home_credit_default_risk_application_train"

spark = SparkSession.builder.config('spark.jars', '/opt/bitnami/spark/jars/mysql-connector-j-8.0.31.jar').appName('app').getOrCreate()
df = spark.read.format('csv').option('header','True').load(path1)
df1 = spark.read.format('csv').option('header','True').load(path2) 
df.show(1)

df.write\
        .format('jdbc') \
        .mode('overwrite')\
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name1) \
        .option("user", SQL_USERNAME) \
        .option("password", SQL_PASSWORD) \
        .save()
        
df1.write\
        .format('jdbc') \
        .mode('overwrite')\
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table_name2) \
        .option("user", SQL_USERNAME) \
        .option("password", SQL_PASSWORD) \
        .save()