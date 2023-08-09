package query

import org.apache.spark.sql.{DataFrame, functions}

object Queries {

  def mostPopularAnime(dataFrame: DataFrame, n: Int): Unit = {
    dataFrame
      .select("title", "popularity")
      .dropDuplicates()
      .sort(dataFrame("popularity"))
      .limit(n)
      .show(false)
  }

  def genderDistribution(df: DataFrame): Unit = {
    df.groupBy(df("gender"))
      .agg(functions.count(df("profile")).alias("Occurence"),
           (functions.count(df("profile")) / df.count() * 100)
             .alias("Percentage"))
      .show(false)
  }

  def mostReviewedAnimes(reviewDf: DataFrame,
                         animeDf: DataFrame,
                         n: Int): Unit = {
    reviewDf
      .join(animeDf, reviewDf("anime_uid") === animeDf("uid"))
      .groupBy(reviewDf("anime_uid"))
      .agg(functions.count("*").alias("Occurence"),
           functions.first("title").alias("Anime Name"))
      .orderBy(functions.desc("Occurence"))
      .select("Anime Name", "Occurence")
      .limit(n)
      .show(false)
  }

  def numberOfFavouriteAnime(profileDf: DataFrame, n: Int): Unit = {
    profileDf
      .withColumn("Number of Favourite Anime",
                  functions.size(profileDf("favorites_anime")))
      .select("profile", "Number of Favourite Anime")
      .orderBy(functions.desc("Number of Favourite Anime"))
      .limit(n)
      .show(false)
  }

  def mostFavouriteAnime(profileDf: DataFrame,
                         animeDf: DataFrame,
                         n: Int): Unit = {
    profileDf
      .select(functions.explode(profileDf("favorites_anime")).alias("uid"))
      .groupBy("uid")
      .count()
      .join(animeDf, "uid")
      .select("title", "count")
      .orderBy(functions.desc("count"))
      .limit(n)
      .show()
  }

}
