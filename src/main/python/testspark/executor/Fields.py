import time
from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

df = spark.createDataFrame([
    [1, 2.2, "aa"], [4, 5.1, "bb"]
], schema=StructType([
    StructField("A", IntegerType(), False),
    StructField("B", DoubleType(), False),
    StructField("C", StringType(), False),
]))

params = {
    "sign": "+",
    "fields": [("A", "aliasA"), ("B", "aliasB"), ("B", None)]
}


def elapsed_time(f):
    def _(*args):
        start_time = time.time_ns()
        result = f(*args)
        end_time = time.time_ns()
        print((end_time - start_time) * 1e-6, " ms")
        return result

    return _


@elapsed_time
def execute(df: DataFrame, params: Dict[str, Any]) -> DataFrame:
    sign = params["sign"]
    fields = params["fields"]

    original_field_names = df.columns

    if sign == "-":
        tmp = list(map(lambda x: x[0], fields))
        selected_fields_and_alias = [
            (field_name, field_name) for field_name in original_field_names if field_name in tmp]
    else:
        selected_fields_and_alias = [
            (f, a) if a else (f, f) for f, a in fields if f in original_field_names
        ]
    if len(selected_fields_and_alias) == 0:
        raise Exception("no column")

    select_fields = list(map(lambda x: col(x[0]).alias(x[1]), selected_fields_and_alias))
    return df.select(*select_fields)


result = execute(df, params)

elapsed_time(lambda x: x.show())(result)
elapsed_time(lambda x: x.show())(result)
elapsed_time(lambda x: x.show())(result)

elapsed_time(lambda x: x.count())(result)
elapsed_time(lambda x: x.count())(result)
elapsed_time(lambda x: x.count())(result)
