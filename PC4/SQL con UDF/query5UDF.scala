import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

val containsUDF: UserDefinedFunction = udf((field: String, value: String) => field != null && field.toLowerCase.contains(value.toLowerCase))
// Register the UDF
spark.udf.register("contains", containsUDF)

val startTime = System.currentTimeMillis()
val agileAndBachelorsJobs = spark.sql("""
  SELECT job_name, job_location, search_city, search_country
  FROM linkedin_jobs
  WHERE contains(knowledge, 'Agile') AND academic_degrees = 'Bachelors'
""")
agileAndBachelorsJobs.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)