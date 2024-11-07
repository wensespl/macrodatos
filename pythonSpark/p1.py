from pyspark.sql import SparkSession

YOUR_SPARK_HOME = r"C:\spark\bin"

logFile = YOUR_SPARK_HOME + r"\Sales_January_2019.csv"  # Should be some file on your system
spark = SparkSession.builder.appName("P1App").getOrCreate()

logData = spark.read.text(logFile).cache()
print(logData.count())

spark.stop()

# .\spark-submit "C:\Users\USUARIO\Desktop\macrodatos\pythonSpark\p1.py"