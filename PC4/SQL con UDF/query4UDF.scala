import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

val df = spark.read.format("jdbc")
  .option("url", "jdbc:postgresql:postgres")
  .option("dbtable", "linkedin_jobs")
  .option("user", "postgres")
  .option("password", "123456")
  .load()
df.createOrReplaceTempView("linkedin_jobs")

val countSkills: UserDefinedFunction = udf((skills: String) => {
  if (skills == null || skills == "No found") 0 else skills.split("-").length
})

// Register the UDF
spark.udf.register("countSkills", countSkills)

val startTime = System.currentTimeMillis()
val averageNumberSkillsJobLevelUDF = spark.sql("""
  SELECT job_level, AVG(countSkills(soft_skills)) as avg_skills
  FROM linkedin_jobs
  GROUP BY job_level
""")
averageNumberSkillsJobLevelUDF.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

// spark.stop()
System.exit(0)