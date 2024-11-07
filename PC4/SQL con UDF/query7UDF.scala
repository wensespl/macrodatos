
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

val df = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql:postgres")
  .option("dbtable", "linkedin_jobs")
  .option("user", "postgres")
  .option("password", "123456")
  .load()
df.createOrReplaceTempView("linkedin_jobs")

val extractKnowledge: UserDefinedFunction = udf((knowledge: String) => knowledge.split("-"))

// Register the UDF
spark.udf.register("extractKnowledge", extractKnowledge)

val startTime = System.currentTimeMillis()
val mostCommonJobLevelUDF = spark.sql("""
  SELECT knowledge_area, COUNT(*) as count
  FROM (
    SELECT explode(extractKnowledge(knowledge)) as knowledge_area
    FROM linkedin_jobs
  )
  GROUP BY knowledge_area
  ORDER BY count DESC
""")
mostCommonJobLevelUDF.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

System.exit(0)