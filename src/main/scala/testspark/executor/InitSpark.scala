package testspark.executor

import org.apache.spark.sql.SparkSession

object InitSpark {
  implicit val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Test")
    .getOrCreate()
}
