package testspark.executor

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object Fields {
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
}
