import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.IntegerType

object DataPreprocessor {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("myApp")
    .master("local[*]")
    .getOrCreate()

  val extractStringArray: UserDefinedFunction = udf(
    (s: String) =>
      s.replace("[", "")
        .replace("]", "")
        .split(", ")
        .map(_.trim))
  spark.udf.register("extractStringArray", extractStringArray)

  val extractIntegerArray: UserDefinedFunction = udf(
    (s: String) =>
      s.replace("[", "")
        .replace("]", "")
        .split(", ")
        .map(_.replace("'", ""))
        .map(_.toIntOption)
  )
  spark.udf.register("extractIntegerArray", extractIntegerArray)

  def cleanAnimeDf(df: DataFrame): DataFrame = {
    df.dropDuplicates()
      .withColumn("genre", extractStringArray(df("genre")))
      .withColumns(Map(("episodes", df("episodes").cast(IntegerType)),
                       ("ranked", df("ranked").cast(IntegerType))))
  }

  def cleanProfileDf(df: DataFrame): DataFrame = {
    df.dropDuplicates()
      .na
      .fill("Not Specifed", Seq("gender", "birthday"))
      .withColumn("favorites_anime", extractIntegerArray(df("favorites_anime")))
  }
  def cleanReviewDf(df: DataFrame): DataFrame = {
    df.dropDuplicates()
      .withColumn("text",
                  functions.trim(
                    functions.regexp_replace(
                      functions.regexp_replace(df("text"), "\\s+", " "),
                      "\n",
                      "")))
  }
}
