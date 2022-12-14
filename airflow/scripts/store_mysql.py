from pyspark.sql import SparkSession



CONNECTOR_TYPE = 'com.mysql.jdbc.Driver'
SQL_USERNAME = 'root'
SQL_PASSWORD = 'admin'
SQL_DBNAME = 'Final_Project'
SQL_SERVERNAME = 'localhost'

url = "jdbc:mysql://host.docker.internal:3306/Final_Project?allowPublicKeyRetrieval=true&useSSL=False"
#path variable
path1 = '/usr/local/spark/resources/application_test.csv'
path2 = '/usr/local/spark/resources/application_train.csv'
#table name
table_name1 = "home_credit_default_risk_application_test_ok"
table_name2 = "home_credit_default_risk_application_train_ok"
#read csv file
if __name__ == '__main__':
        spark = SparkSession.builder.config('spark.jars', '/usr/local/spark/resources/mysql-connector-j-8.0.31.jar').appName('app').getOrCreate()
        data = spark.read.format('csv').option('header','True').load(path1)
        data1 = spark.read.format('csv').option('header','True').load(path2) 
     
        #write data to mysql
        data.write \
                .format("jdbc") \
                .mode("overwrite")\
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("url", "jdbc:mysql://host.docker.internal:3306/Final_Project?user=root&password=admin&useUnicode=true&characterEncoding=UTF-8") \
                .option("dbtable", "home_credit_default_risk_application_test_ok") \
                .option("user", "root") \
                .option("password", "admin") \
                .save()
                
        data1.write \
                .format("jdbc") \
                .mode("overwrite")\
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("url", "jdbc:mysql://host.docker.internal:3306/Final_Project?user=root&password=admin&useUnicode=true&characterEncoding=UTF-8") \
                .option("dbtable", "home_credit_default_risk_application_train_ok") \
                .option("user", "root") \
                .option("password", "admin") \
                .save()