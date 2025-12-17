df = spark.read.format("delta").load("/Volumes/workspace/default/dataengineer/gold/sales/")

df1 = df.orderBy("month")

display(df1)