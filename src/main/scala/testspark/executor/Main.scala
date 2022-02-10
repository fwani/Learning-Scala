package testspark.executor

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import testspark.executor.Fields.execute
import testspark.executor.InitSpark.spark

import java.lang.System.nanoTime

object Main {

  def getDataFrame(data: List[Row])(schema: List[StructField])(implicit spark: SparkSession): DataFrame = {
    val dataRDD = spark.sparkContext.parallelize(data)
    spark.createDataFrame(dataRDD, schema = StructType(schema))
  }

  def elapsedTime[R](block: => R): R = {
    val startTime = nanoTime()
    val result = block
    val endTime = nanoTime()
    println((endTime - startTime) * 1e-6 + " ms")
    result
  }

  def main(args: Array[String]): Unit = {
    val paramsStack = List(
      Map("sign" -> "+", "fields" -> List(("A", "aliasA"), ("B", "aliasB"), ("C", null))),
      Map("sign" -> "-", "fields" -> List(("A", null), ("C", null))),
    )

    val df = getDataFrame {
      List(
        Row(1, 2.2, "aa"),
        Row(4, 5.1, "bb"),
      )
    } {
      List(
        StructField("A", IntegerType, false),
        StructField("B", DoubleType, false),
        StructField("C", StringType, false),
      )
    }

    paramsStack.foreach(
      param => {
        val result = elapsedTime {
          execute(df, param)
        }
        elapsedTime {
          result.show()
        }
      }
    )
  }
}
