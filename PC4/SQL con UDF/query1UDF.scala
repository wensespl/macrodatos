import org.apache.spark.sql.functions._

val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

// Definir la UDF
val skillsAndLanguagesUDF = udf((softSkills: String, programmingLanguages: String) => {
  val requiredSkill = "Teamwork"
  val requiredLanguages = List("Python", "Java")
  softSkills.contains(requiredSkill) && requiredLanguages.forall(programmingLanguages.contains)
})

// Registrar la UDF
spark.udf.register("skillsAndLanguagesUDF", skillsAndLanguagesUDF)

// Usar la UDF en una consulta SQL
val startTime = System.currentTimeMillis()

val result = spark.sql("""
  SELECT job_name, programming_languages, soft_skills 
  FROM linkedin_jobs 
  WHERE skillsAndLanguagesUDF(soft_skills, programming_languages)
""")

result.show()

val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

System.exit(0)