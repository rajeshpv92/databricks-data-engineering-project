from pyspark.sql.functions import to_date, date_trunc, sum as _sum
df_silver = spark.read.format("delta").load("/Volumes/workspace/default/dataengineer/silver/sales/")
df_monthly = df_silver.withColumn("month", date_trunc("month", to_date("sale_date"))) \
                     .groupBy("month","product_id","region") \
                     .agg(_sum("revenue").alias("total_revenue"), _sum("quantity").alias("total_qty"))
df_monthly.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/dataengineer/gold/sales/")
