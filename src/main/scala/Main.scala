import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val spark = SparkSession
      .builder()
      .appName("myApp")
      .master("local[*]")
      .getOrCreate()

    //READ DATA
    val rawAnimeData = SparkUtils.readCSV(
      spark,
      filePath = config.getString("csv-paths.animes"),
      Schema.Animes)

    val rawProfileData = SparkUtils.readCSV(
      spark,
      filePath = config.getString("csv-paths.profiles"),
      Schema.Profiles)

    val rawReviewData = SparkUtils.readCSV(
      spark,
      filePath = config.getString("csv-paths.reviews"),
      Schema.Reviews)

    //PREPROCESSING
    val dataPp = DataPreprocessor
    val cleanedAnimeDf = dataPp.cleanAnimeDf(rawAnimeData)
    val cleanedProfileDf = dataPp.cleanProfileDf(rawProfileData)
    val cleanedReviewDf = dataPp.cleanReviewDf(rawReviewData)

  }
}
