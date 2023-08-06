package sparkutils


import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  def readCSV(spark: SparkSession, filePath: String, schema: StructType): DataFrame = {
    spark.read.schema(schema)
      .option("header", "true")
      .option("multiLine", "true")
      .option("sep", ",")
      .option("parserLib", "UNIVOCITY")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(filePath)
  }

}
