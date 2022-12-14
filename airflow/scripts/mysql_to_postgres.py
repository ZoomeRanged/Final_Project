
from pyspark.sql import SparkSession





#read data from mysql DB
if __name__ == '__main__':
        spark = SparkSession.builder.config('spark.jars', '/usr/local/spark/resources/mysql-connector-j-8.0.31.jar, /usr/local/spark/resources/postgresql-42.5.1.jar').appName('app').getOrCreate()
        app_test = spark.read\
                .format('jdbc') \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("url", "jdbc:mysql://host.docker.internal:3306/Final_Project") \
                .option("dbtable", "home_credit_default_risk_application_test_ok") \
                .option("user", 'root') \
                .option("password", 'admin') \
                .load()
                
        app_train = spark.read\
                .format('jdbc') \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("url", "jdbc:mysql://host.docker.internal:3306/Final_Project") \
                .option("dbtable", "home_credit_default_risk_application_train_ok") \
                .option("user", 'root') \
                .option("password", 'admin') \
                .load()
        
        


        #write to postgres DB
        
        

        app_test.write\
                .format('jdbc') \
                .option('driver', 'org.postgresql.Driver') \
                .option("url", "jdbc:postgresql://host.docker.internal:5434/postgres") \
                .option("dbtable", "home_credit_default_risk_application_test_ok") \
                .option("user", 'postgres') \
                .option("password", 'admin') \
                .mode("overwrite") \
                .save()
        
        app_train.write\
                .format('jdbc') \
                .option('driver', 'org.postgresql.Driver') \
                .option("url", "jdbc:postgresql://host.docker.internal:5434/postgres") \
                .option("dbtable", "home_credit_default_risk_application_train_ok") \
                .option("user", 'postgres') \
                .option("password", 'admin') \
                .mode('overwrite') \
                .save()
       

