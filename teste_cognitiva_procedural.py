from pyspark.sql.window import Window
from pyspark.sql.functions import col
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import max as max_


conf = SparkConf()
spark = (SparkSession.builder.config(conf=conf).getOrCreate())

df_conversao = spark.read.csv("/home/jennifer/teste-eng-dados/data/input/users/load.csv", sep=",",header="True")
df_conversao.repartition(1).write.parquet("/tmp/data/output/parquet/", mode="overwrite")
df_conversao_parquet = spark.read.parquet("/tmp/data/output/parquet/")
df_conversao_parquet.show()

df_conversao_parquet.groupBy("id").agg(max_("update_date"))
window_rules = Window().partitionBy("id").orderBy(f.col("update_date").desc())
df_deduplicacao = df_conversao_parquet.select("id","name","email","phone","address","age","create_date","update_date", f.row_number().over(window_rules).alias('most_recent')).drop("most_recent").filter("most_recent==1").orderBy("id")
df_deduplicacao.show()


df_schema = df_deduplicacao
df_schema = df_schema.withColumnRenamed("age", "old_age")
df_schema = df_schema.withColumn("age", col("old_age").cast("integer")).drop("old_age")
df_schema = df_schema.withColumnRenamed("create_date", "old_create_date")
df_schema = df_schema.withColumn("create_date", col("old_create_date").cast("timestamp")).drop("old_create_date")
df_schema = df_schema.withColumnRenamed("update_date", "old_update_date")
df_schema = df_schema.withColumn("update_date", col("old_update_date").cast("timestamp")).drop("old_update_date")
df_schema.printSchema()


