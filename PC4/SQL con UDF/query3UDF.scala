import org.apache.spark.sql.functions._

val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

// Definir la UDF
val containsCertifiedUDF = udf((certifications: String) => certifications != null && certifications.toLowerCase.contains("certified"))
val containsWebDevelopmentUDF = udf((knowledge: String) => knowledge != null && knowledge.toLowerCase.contains("web development"))

// Registrar las UDFs
spark.udf.register("containsCertified", containsCertifiedUDF)
spark.udf.register("containsWebDevelopment", containsWebDevelopmentUDF)

// Usar las UDFs en una consulta SQL
val startTime = System.currentTimeMillis()

val result = spark.sql("""
  SELECT job_name, knowledge, certifications
  FROM linkedin_jobs
  WHERE containsWebDevelopment(knowledge) AND containsCertified(certifications)
""")

result.show()

val endTime = System.currentTimeMillis()
val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

// spark.stop()
System.exit(0)
