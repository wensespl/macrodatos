import org.apache.spark.sql.functions._

val df = spark.read.format("jdbc")
  .option("url", "jdbc:postgresql:postgres")
  .option("dbtable", "linkedin_jobs")
  .option("user", "postgres")
  .option("password", "123456")
  .load()

val df_skill = df
    .withColumn("skills_merged", 
        concat_ws("-",
            $"programming_languages", 
            $"technologies")
    )
    .withColumn("skill", 
        explode(split($"skills_merged", "-"))
    )
    .filter($"skill" !== "No found")
    .groupBy("skill")
    .count()
    .orderBy(desc("count"))
    .select("skill", "count")
println(s"Total skills: ${df_skill.count()}")
df_skill.show()

val quartiles = df_skill
    .stat.approxQuantile("count", Array(0.25, 0.5, 0.75), 0.0)
println(s"Quartiles: ${quartiles.mkString(", ")}")

// df_skill
//     .filter($"count" === quartiles(0) || 
//             $"count" === quartiles(1) || 
//             $"count" === quartiles(2))
//     .show()

val df_skill_quartiles = df_skill
    .withColumn("skill_quartile", 
        when($"count" <= quartiles(0), "Q4")
        .when($"count" <= quartiles(1), "Q3")
        .when($"count" <= quartiles(2), "Q2")
        .otherwise("Q1")
    )
    .groupBy("skill_quartile")
    .agg(
        collect_list("skill").as("skills")
    )
    .orderBy("skill_quartile")
df_skill_quartiles.show()

// df_skill_quartiles
//     .foreach(row => {
//         val skill_quartile = row.getAs[String]("skill_quartile")
//         val skills = row.getAs[Seq[String]]("skills")
//         println(s"Quartile: $skill_quartile \n ${skills.foreach(println)}")
//     })

// spark.stop()
System.exit(0)