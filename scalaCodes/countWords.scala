// import org.apache.spark.SparkContext

def main(args: Array[String]): Unit = {
  val sc = new SparkContext("local", "countWords", System.getenv("SPARK_HOME"))

  val txtShake = sc.textFile("shake.txt")
  txtShake.collect

  val query = txtShake.flatMap(_.split(" ")).map(_.replaceAll("[^A-Za-z0-9]", "")).map((_,1)).reduceByKey(_ + _);
  query.toDebugString
  query.cache()

  query.saveAsTextFile("output_shake_")
  query.collect
}
// val sc = new SparkContext("local", "countWords", System.getenv("SPARK_HOME"))

// val txtShake = sc.textFile("shake.txt")
// txtShake.collect

// val query = txtShake.flatMap(_.split(" ")).map(_.replaceAll("[^A-Za-z0-9]", "")).map((_,1)).reduceByKey(_ + _);
// query.toDebugString
// query.cache()

// query.saveAsTextFile("output_shake_")
// query.collect