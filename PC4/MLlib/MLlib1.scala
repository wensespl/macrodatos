import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, CrossValidator}
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
jobDF.select("num_job_type", "n_languages", "n_certifications", "n_soft_skills", "n_programming_languages", "n_technologies", "n_knowledge", "n_academic_degrees").show(10)

// Data splitting
val splitSeed = 54321
val Array(trainingData, testData) = jobDF.randomSplit(Array(0.80, 0.20), splitSeed)

println(s"Training Data count: ${trainingData.count}")
println(s"Test Data count: ${testData.count}")

// Define the stages of the Pipeline

// StringIndexer for categorical features
// Label indexing
val labelIndexer = new StringIndexer()
  .setInputCol("num_job_type")
  .setOutputCol("label")

// Assembler to combine feature columns into a single feature vector
// Feature Assembler
val featureCols = Array("n_languages", "n_certifications", "n_soft_skills", "n_programming_languages", "n_technologies", "n_knowledge", "n_academic_degrees")
val featureAssembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")

val lr = new LogisticRegression()
  .setLabelCol("num_job_type")
  .setMaxIter(20)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)
  .setFamily("multinomial")

val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, featureAssembler, lr))

// Train the model using the training data
val lrModel = pipeline.fit(trainingData)

// Make predictions on the test data
val predictions = lrModel.transform(testData)
predictions.select("label", "prediction", "probability").show(10)

val model = lrModel.stages.last.asInstanceOf[LogisticRegressionModel]

println("Metrics before hyperparameter tuning: ")
println(s"Coefficients: \n${model.coefficientMatrix}")
println(s"Intercepts: \n${model.interceptVector}")

val modelSummary = model.summary

println("objectiveHistory:")
modelSummary.objectiveHistory.foreach(println)

println("False positive rate by label:")
modelSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("True positive rate by label:")
modelSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("Precision by label:")
modelSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>
  println(s"label $label: $prec")
}

println("Recall by label:")
modelSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
  println(s"label $label: $rec")
}

println("F-measure by label:")
modelSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
  println(s"label $label: $f")
}

println(s"Accuracy: ${modelSummary.accuracy}")
println(s"False positive rate: ${modelSummary.weightedFalsePositiveRate}")
println(s"True positive rate: ${modelSummary.weightedTruePositiveRate}")
println(s"F-measure: ${modelSummary.weightedFMeasure}")
println(s"Precision: ${modelSummary.weightedPrecision}")
println(s"Recall: ${modelSummary.weightedRecall}")

// Perform hyperparameter tuning using CrossValidator
val paramGrid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.3, 0.2, 0.1, 0.01))
  .addGrid(lr.elasticNetParam, Array(0.0, 0.25, 0.5, 0.75, 1.0))
  .build()

val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val validator = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(4)

// Run validation, and choose the best set of parameters.
val modelTuned = validator.fit(trainingData)

// Make predictions on the test data with the tuned model
val predictionsTuned = modelTuned.transform(testData)

val bestModel = modelTuned.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[LogisticRegressionModel]

// println("Best Model Parameters: ")
// println(bestModel.explainParams())

println("Metrics after hyperparameter tuning: ")
println(s"Coefficients: \n${bestModel.coefficientMatrix}")
println(s"Intercepts: \n${bestModel.interceptVector}")

val bestModelSummary = bestModel.summary

println("objectiveHistory:")
bestModelSummary.objectiveHistory.foreach(println)

println("False positive rate by label:")
bestModelSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("True positive rate by label:")
bestModelSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("Precision by label:")
bestModelSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>
  println(s"label $label: $prec")
}

println("Recall by label:")
bestModelSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
  println(s"label $label: $rec")
}

println("F-measure by label:")
bestModelSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
  println(s"label $label: $f")
}

println(s"Accuracy: ${bestModelSummary.accuracy}")
println(s"False positive rate: ${bestModelSummary.weightedFalsePositiveRate}")
println(s"True positive rate: ${bestModelSummary.weightedTruePositiveRate}")
println(s"F-measure: ${bestModelSummary.weightedFMeasure}")
println(s"Precision: ${bestModelSummary.weightedPrecision}")
println(s"Recall: ${bestModelSummary.weightedRecall}")

println("Model Parameters Summary: ")
println(s"RegParam: ${bestModel.getRegParam}")
println(s"ElasticNetParam: ${bestModel.getElasticNetParam}")
println(s"MaxIter: ${bestModel.getMaxIter}")

spark.stop()
System.exit(0)