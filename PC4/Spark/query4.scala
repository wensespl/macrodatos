import org.apache.spark.sql.functions.{col, split, size, when}

val df = spark.read.format("jdbc")
  .option("url", "jdbc:postgresql:postgres")
  .option("dbtable", "linkedin_jobs")
  .option("user", "postgres")
  .option("password", "123456")
  .load()

val startTime = System.currentTimeMillis()
val avgSkillsByJobLevel = df
  .withColumn("skills_count", 
    when(col("soft_skills").isNull.or(col("soft_skills") === "No found"), 0)
    .otherwise(size(split(col("soft_skills"), "-")))
  )
  .groupBy("job_level")
  .agg("skills_count" -> "avg")
  .withColumnRenamed("avg(skills_count)", "avg_skills")
avgSkillsByJobLevel.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

// spark.stop()
System.exit(0)