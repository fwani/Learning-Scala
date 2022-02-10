import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.lang.System.nanoTime

def execute(df: DataFrame, params: Map[String, Any]): DataFrame = {
  val sign = params("sign").asInstanceOf[String]
  val fields = params("fields").asInstanceOf[List[(String, String)]]

  val originalFieldNames = df.columns.toList

  val selectedFieldsAndAlias: List[(String, String)] =
    if (sign == "-") {
      originalFieldNames
        .filter(! fields.map(_._1).contains(_))
        .map(x => (x, x))
    } else {
      fields
        .filter(x => originalFieldNames.contains(x._1))
        .map(x => x._2 match {
          case alias: String => (x._1, alias)
          case _ => (x._1, x._1)
        })
    }

  if (selectedFieldsAndAlias.isEmpty) throw new Exception("no column")

  val selectFields = selectedFieldsAndAlias.map(
    x => col(x._1).alias(x._2)
  )
  df.select(selectFields: _*)
}

val spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

val dataRDD = spark.sparkContext.parallelize(List(Row(1, 2.2, "aa"), Row(4, 5.1, "bb")))

val df = spark.createDataFrame(dataRDD,
  schema = StructType(List(
    StructField("A", IntegerType, false),
    StructField("B", DoubleType, false),
    StructField("C", StringType, false),
  )))

val params: Map[String, Any] = Map(
  "sign" -> "+",
  "fields" -> List(("A", "aliasA"), ("B", "aliasB"), ("B", null))
)

def elapsedTime[R](block: => R): R = {
  val startTime = nanoTime()
  val result = block
  val endTime = nanoTime()
  println((endTime - startTime) * 1e-6 + " ms")
  result
}

val result = elapsedTime{
  execute(df, params)
}
elapsedTime{
  result.show()
}
elapsedTime{
  result.show()
}
elapsedTime{
  result.show()
}

elapsedTime{
  result.count()
}
elapsedTime{
  result.count()
}
elapsedTime{
  result.count()
}

spark.sparkContext.cancelJobGroup("a")

spark.close()