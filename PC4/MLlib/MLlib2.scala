import org.apache.spark.ml.classification.{DecisionTreeClassifier, DecisionTreeClassificationModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.functions._
import spark.implicits._

sc.setLogLevel("ERROR")  // Set the log level to ERROR to avoid warnings

val rawRDD = sc.textFile("linkedin_jobs.csv")

case class Job(
    job_location: String,
    search_city: String, 
    search_country: String, 
    job_level: String, 
    job_type: String,
    job_name: String, 
    languages: String, 
    certifications: String, 
    soft_skills: String, 
    programming_languages: String, 
    technologies: String,
    academic_degrees: String, 
    knowledge: String, 
    n_languages: Double, 
    n_certifications: Double,
    n_soft_skills: Double, 
    n_programming_languages: Double, 
    n_technologies: Double, 
    n_knowledge: Double, 
    n_academic_degrees: Double,
    num_job_level: Double,
    num_job_type: Double,
    num_search_country: Double
)

def parseLine(line: String): Job = {
  val fields = line.split(",")
  
  Job(
      fields(0), fields(1), fields(2), fields(3), fields(4), fields(5),
      fields(6), fields(7), fields(8), fields(9), fields(10), fields(11),
      fields(12), fields(13).toDouble, fields(14).toDouble, fields(15).toDouble,
      fields(16).toDouble, fields(17).toDouble, fields(18).toDouble, line(19).toDouble, 
      fields(20).toDouble, fields(21).toDouble, fields(22).toDouble
      )
}

// Skip the header row (if your CSV has a header)
val header = rawRDD.first()
val dataRDD = rawRDD.filter(row => row != header)

val jobRDD = dataRDD.map(parseLine)

val jobDF = jobRDD.toDF().cache()
jobDF.printSchema()
println(s"Data count: ${jobDF.count}")
jobDF.select("num_job_level", "n_languages", "n_certifications", "n_soft_skills", "n_programming_languages", "n_technologies", "n_knowledge", "n_academic_degrees").show(10)

// Data splitting
val splitSeed = 54321
val Array(trainingData, testData) = jobDF.randomSplit(Array(0.80, 0.20), splitSeed)

println(s"Training Data count: ${trainingData.count}")
println(s"Test Data count: ${testData.count}")

// Define the stages of the Pipeline

// StringIndexer for categorical features
// Label indexing
val labelIndexer = new StringIndexer()
  .setInputCol("num_job_level")
  .setOutputCol("label")

// Assembler to combine feature columns into a single feature vector
// Feature Assembler
val featureCols = Array("n_languages", "n_certifications", "n_soft_skills", "n_programming_languages", "n_technologies", "n_knowledge", "n_academic_degrees")
val featureAssembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

val dt = new DecisionTreeClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")

val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureAssembler, dt))

// Train the model using the training data
val dtModel = pipeline.fit(trainingData)

// Make predictions on the test data
val predictions = dtModel.transform(testData)
predictions.select("label", "prediction", "probability").show(10)

val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("prediction")

val aucROCEvaluator = evaluator.setMetricName("areaUnderROC")
val aucPRCEvaluator = evaluator.setMetricName("areaUnderPR")

println("Metrics before hyperparameter tuning: ")
println(s"Test Error = ${1.0 - evaluator.evaluate(predictions)}")
println(s"Accuracy = ${evaluator.evaluate(predictions)}")
println(s"Area Under ROC before tuning: ${aucROCEvaluator.evaluate(predictions)}")
println(s"Area Under PRC before tuning: ${aucPRCEvaluator.evaluate(predictions)}")

// Perform hyperparameter tuning using CrossValidator
val paramGrid = new ParamGridBuilder()
  .addGrid(dt.maxDepth, Array(5, 10))
  .addGrid(dt.impurity, Array("gini", "entropy"))
  .build()

val validator = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(4)

// Run validation, and choose the best set of parameters.
val modelTuned = validator.fit(trainingData)

// Make predictions on the test data with the tuned model
val predictionsTuned = modelTuned.transform(testData)

// Evaluate the tuned model
println("\nMetrics after hyperparameter tuning: ")
println(s"Tuned Test Error = ${1.0 - evaluator.evaluate(predictionsTuned)}")
println(s"Tuned accuracy = ${evaluator.evaluate(predictionsTuned)}")
println(s"Area Under ROC after tuning: ${aucROCEvaluator.evaluate(predictionsTuned)}")
println(s"Area Under PRC after tuning: ${aucPRCEvaluator.evaluate(predictionsTuned)}")

val bestModel = modelTuned.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[DecisionTreeClassificationModel]

// println("Best Model Parameters: ")
// println(bestModel.explainParams())

println("Model Parameters Summary: ")
println(s"Best maxDepth: ${bestModel.getMaxDepth}")
println(s"Best impurity: ${bestModel.getImpurity}")

println("Learned classification tree model: ")
println(bestModel.toDebugString)

spark.stop()
System.exit(0)