import org.apache.spark.sql.functions._

val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

// Definir la UDF
val locationAndDegreeUDF = udf((searchCity: String, academicDegrees: String) => {
  searchCity == "Cincinnati" && !academicDegrees.contains("No Found")
})

// Registrar la UDF
spark.udf.register("locationAndDegreeUDF", locationAndDegreeUDF)

// Usar la UDF en una consulta SQL
val startTime = System.currentTimeMillis()

val result = spark.sql("""
  SELECT job_name, COUNT(*) as job_count
  FROM linkedin_jobs 
  WHERE locationAndDegreeUDF(search_city, academic_degrees)
  GROUP BY job_name
  ORDER BY job_count DESC
  LIMIT 5
""")

result.show()

val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)