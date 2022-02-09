import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val spark = SparkSession.builder.appName("Test").getOrCreate()

val df = spark.createDataFrame(RowRDD(Row(1,2,3), Row(4,5,6)),
  schema = StructType(List(
    StructField("A", StringType, false),
    StructField("B", StringType, false),
    StructField("C", StringType, false),
  )))