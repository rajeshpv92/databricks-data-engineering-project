df = spark.read.format("delta").load("/Volumes/workspace/default/dataengineer/bronze/sales/")
df = df.withColumn("quantity", df["quantity"].cast("int")) \
       .withColumn("unit_price", df["unit_price"].cast("double")) \
       .withColumn("revenue", df["revenue"].cast("double"))
# example dedupe on order_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
w = Window.partitionBy("order_id").orderBy(df["created_at"].desc())
df_clean = df.withColumn("rn", row_number().over(w)).filter("rn = 1").drop("rn")
df_clean.write.format("delta").mode("overwrite").save("/Volumes/workspace/default/dataengineer/silver/sales/")
