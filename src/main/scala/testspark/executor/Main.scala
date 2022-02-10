package testspark.executor

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import testspark.executor.Fields.execute

import java.lang.System.nanoTime

object Main {
  def main(args: Array[String]): Unit = {
    val paramsStack = List(
      Map("sign" -> "+", "fields" -> List(("A", "aliasA"), ("B", "aliasB"), ("C", null))),
      Map("sign" -> "-", "fields" -> List(("A", null), ("C", null))),
    )
    implicit val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("Test")
      .getOrCreate()

    val dataRDD = spark.sparkContext.parallelize(List(Row(1, 2.2, "aa"), Row(4, 5.1, "bb")))

    val df = spark.createDataFrame(dataRDD,
      schema = StructType(List(
        StructField("A", IntegerType, false),
        StructField("B", DoubleType, false),
        StructField("C", StringType, false),
      )))


    paramsStack.foreach(
      param => {
        val result = elapsedTime{
          execute(df, param)
        }
        elapsedTime{
          result.show()
        }
      }
    )
  }

  def elapsedTime[R](block: => R): R = {
    val startTime = nanoTime()
    val result = block
    val endTime = nanoTime()
    println((endTime - startTime) * 1e-6 + " ms")
    result
  }
}
