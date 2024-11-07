import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction

val df = spark.read.format("jdbc")
    .option("url", "jdbc:postgresql:postgres")
    .option("dbtable", "linkedin_jobs")
    .option("user", "postgres")
    .option("password", "123456")
    .load()
df.createOrReplaceTempView("linkedin_jobs")

val splitLanguages: UserDefinedFunction = udf((languages: String) => languages.split("-"))
// Register the UDF with Spark SQL
spark.udf.register("splitLanguages", splitLanguages)

val startTime = System.currentTimeMillis()
val mostCommonProgrammingLanguage = spark.sql("""
  SELECT exploded_languages AS programming_language, COUNT(*) AS count
  FROM (
    SELECT explode(splitLanguages(programming_languages)) AS exploded_languages
    FROM linkedin_jobs
    WHERE job_type = 'Onsite'
  )
  GROUP BY exploded_languages
  ORDER BY count DESC
  LIMIT 1
""")
mostCommonProgrammingLanguage.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)