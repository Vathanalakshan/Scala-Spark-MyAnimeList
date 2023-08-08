import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import preprocessing.DataPreprocessor
import query.Queries
import schema.Schema
import sparkutils.SparkUtils

object Main {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val spark = SparkSession
      .builder()
      .appName("myApp")
      .master("local[*]")
      .getOrCreate()

    spark.time {

      //READ DATA
      val rawAnimeData =
        SparkUtils.readCSV(spark,
                           filePath = config.getString("csv-paths.animes"),
                           Schema.Animes)

      val rawProfileData =
        SparkUtils.readCSV(spark,
                           filePath = config.getString("csv-paths.profiles"),
                           Schema.Profiles)

      val rawReviewData =
        SparkUtils.readCSV(spark,
                           filePath = config.getString("csv-paths.reviews"),
                           Schema.Reviews)

      //PREPROCESSING
      val dataPp = DataPreprocessor

      val cleanedAnimeDf = dataPp.cleanAnimeDf(rawAnimeData)
      val cleanedProfileDf = dataPp.cleanProfileDf(rawProfileData)
      val cleanedReviewDf = dataPp.cleanReviewDf(rawReviewData)

      //Queries
      val query = Queries

      println(
        "Number of rows in the Anime DataFrame: " + cleanedAnimeDf.count())
      println(
        "Number of rows in the Profile DataFrame: " + cleanedProfileDf.count())
      println(
        f"Number of rows in the Review DataFrame:" + cleanedReviewDf.count())

      println("The gender distribution is :")
      query.genderDistribution(cleanedProfileDf)

      println("Most popular animes are :")
      query.mostPopularAnime(cleanedAnimeDf, 10)

      println("Most Reviewed Animes are :")
      query.mostReviewedAnimes(cleanedReviewDf, cleanedAnimeDf)
    }
  }
}
