val df = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql:postgres")
    .option("dbtable", "linkedin_jobs")
    .option("user", "postgres")
    .option("password", "123456")
    .load()

val startTime = System.currentTimeMillis()
val knowledgeCountDf = df
    .withColumn("knowledge_area", 
        explode(split(col("knowledge"), "-"))
    )
    .groupBy("knowledge_area")
    .agg(count("*")
        .alias("count")
    )
    .orderBy(col("count").desc)
knowledgeCountDf.show()
val endTime = System.currentTimeMillis()

val executionTime = endTime - startTime
println(s"Execution time: $executionTime milliseconds")

spark.stop()
System.exit(0)