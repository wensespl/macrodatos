// Mostrar los Trabajos que solicitan en Cincinnati con algun tipo de Nivel Académico.

val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

val startTime = System.currentTimeMillis()

val locationAndDegree = df.filter("search_city = 'Cincinnati' AND academic_degrees NOT LIKE '%No Found%'")
  .groupBy("job_name")
  .agg(count("*").as("job_count"))
  .orderBy(desc("job_count"))
  .limit(5)

locationAndDegree.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)