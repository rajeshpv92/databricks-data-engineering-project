from pyspark.sql.functions import current_timestamp
df = spark.read.option("header","true").csv("/Volumes/workspace/default/dataengineer/raw/sales_dump.csv")



df = df.withColumn("ingest_ts", current_timestamp())
df.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/dataengineer/bronze/sales/")

