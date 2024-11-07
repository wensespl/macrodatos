import org.apache.spark.sql.functions._

val df = spark.read.format("jdbc")
                    .option("url", "jdbc:postgresql:postgres")
                    .option("dbtable", "linkedin_jobs")
                    .option("user", "postgres")
                    .option("password", "123456")
                    .load()

val startTime = System.currentTimeMillis()
val agileAndBachelorsJobs = df
  .filter($"knowledge".contains("Agile") && $"academic_degrees" === "Bachelors")
  .select("job_name", "job_location", "search_city", "search_country")
agileAndBachelorsJobs.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)