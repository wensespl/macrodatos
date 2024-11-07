// Mostrar los Trabajos que requieren Web Development como Conocimiento y una Certificación en AWS.
val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

val startTime = System.currentTimeMillis()

// Filtrar trabajos que requieren "Web Development" como conocimiento y alguna certificación que contenga "Certified"
val knowledgeAndCertifications = df.filter("knowledge RLIKE '(?i).*Web Development.*' AND certifications RLIKE '(?i).*Certified.*'")
  .select("job_name", "knowledge", "certifications")

knowledgeAndCertifications.show()

val endTime = System.currentTimeMillis()
val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

// spark.stop()
System.exit(0)