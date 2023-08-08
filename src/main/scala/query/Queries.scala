package query

import org.apache.spark.sql.{DataFrame, functions}

object Queries {

  def mostPopularAnime(dataFrame: DataFrame, limit: Int): Unit = {
    dataFrame
      .select("title", "popularity")
      .dropDuplicates()
      .sort(dataFrame("popularity"))
      .limit(limit)
      .show(false)
  }

  def genderDistribution(df: DataFrame): Unit = {
    df.groupBy(df("gender"))
      .agg(functions.count(df("profile")).alias("Occurence"),
           (functions.count(df("profile")) / df.count() * 100)
             .alias("Percentage"))
      .show(false)
  }

  def mostReviewedAnimes(reviewDf: DataFrame, animeDf: DataFrame): Unit = {
    reviewDf
      .join(animeDf, reviewDf("anime_uid") === animeDf("uid"))
      .groupBy(reviewDf("anime_uid"))
      .agg(functions.count("*").alias("Occurence"),
           functions.first("title").alias("Anime Name"))
      .orderBy(functions.desc("Occurence"))
      .select("Anime Name", "Occurence")
      .limit(5)
      .show(false)
  }
}
