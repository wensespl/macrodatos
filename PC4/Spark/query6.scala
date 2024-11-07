import org.apache.spark.sql.functions._

val df = spark.read.format("jdbc")
                    .option("url", "jdbc:postgresql:postgres")
                    .option("dbtable", "linkedin_jobs")
                    .option("user", "postgres")
                    .option("password", "123456")
                    .load()

val startTime = System.currentTimeMillis()
val mostCommonProgrammingLanguage = df
  .withColumn("programming_language", 
    explode(split($"programming_languages", "-"))
  )
  .filter($"job_type" === "Onsite")
  .groupBy("programming_language")
  .count()
  .orderBy(desc("count"))
  .limit(1)
mostCommonProgrammingLanguage.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)