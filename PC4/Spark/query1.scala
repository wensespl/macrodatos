// Mostrar Trabajos que Requieren Teamwork como Habilidad Blanda y utilizan los Lenguajes de Programación: Python y Java.
val df = spark.read.format("jdbc").option("url", "jdbc:postgresql:postgres").option("dbtable", "linkedin_jobs").option("user", "postgres").option("password", "123456").load()
df.createOrReplaceTempView("linkedin_jobs")

val startTime = System.currentTimeMillis()
// Filtrar trabajos que requieren "Teamwork" y utilizan "Python" y "Java"
// val softSkillAndProgLanguage = df.filter(col("soft_skills") === "Teamwork" && 
//                                         (col("programming_languages") === "Python" || col("programming_languages") === "Java"))
//                                 .groupBy("job_name")
//                                 .count()
//                                 .orderBy(desc("count"))
val softSkillAndProgLanguage = df.filter("soft_skills LIKE '%Teamwork%' AND (programming_languages LIKE '%Python%' AND programming_languages LIKE '%Java%')")
                    .select("job_name", "programming_languages", "soft_skills")
softSkillAndProgLanguage.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)