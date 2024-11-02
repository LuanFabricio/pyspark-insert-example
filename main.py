from hashlib import sha256
import time

from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Spark-PG")\
        .config("spark.jars", "postgresql-42.7.4.jar")\
        .getOrCreate()

df = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/spark_db") \
    .option("dbtable", "public.spark_tb") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver")\
    .load()

print(f"DF: {df}")

print(df.printSchema())

h = sha256(b"senha123")
data = [("luan", h.digest(), h.digest(), 0.0)] * 10_000_000
df = spark.createDataFrame(
        data=data,
        schema=["username", "password_hash", "endereco", "vl_salario"])

print("Inserting...")

start = time.time()

create_table_str: str = "username VARCHAR(50), password_hash CHAR(256), endereco CHAR(256), vl_salario numeric"
dfw = df.write\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/spark_db") \
    .option("createTableColumnTypes", create_table_str) \
    .option("batchsize", "5000") \
    .option("dbtable", "public.spark_tb2") \
    .option("user", "postgres") \
    .option("password", "1234") \
    .option("driver", "org.postgresql.Driver")\
    .save()

end = time.time()

print(f"Delta time: {(end-start)}")

# .jdbc(
#         url="jdbc:postgresql:localhost:5432/spark_db",
#         table="public.spark_tb",
#         properties={
#             "user": "postgres",
#             "password": "1234",
#         }
# ).load()
