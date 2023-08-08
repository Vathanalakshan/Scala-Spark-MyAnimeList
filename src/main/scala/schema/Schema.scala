package schema

import org.apache.spark.sql.types._
object Schema {

  val Animes: StructType = new StructType()
    .add("uid", LongType, nullable = false)
    .add("title", StringType, nullable = false)
    .add("synopsis", StringType, nullable = false)
    .add("genre", StringType, nullable = false)
    .add("aired", StringType, nullable = false)
    .add("episodes", FloatType, nullable = false)
    .add("members", IntegerType, nullable = false)
    .add("popularity", IntegerType, nullable = false)
    .add("ranked", FloatType, nullable = false)
    .add("score", FloatType, nullable = false)
    .add("img_url", StringType, nullable = false)
    .add("link", StringType, nullable = false)

  val Profiles: StructType = new StructType()
    .add("profile", StringType, nullable = false)
    .add("gender", StringType, nullable = false)
    .add("birthday", StringType, nullable = false)
    .add("favorites_anime", StringType, nullable = false)
    .add("link", StringType, nullable = false)

  val Reviews: StructType = new StructType()
    .add("uid", LongType, nullable = false)
    .add("profile", StringType, nullable = false)
    .add("anime_uid", LongType, nullable = false)
    .add("text", StringType, nullable = false)
    .add("score", IntegerType, nullable = false)
    .add("scores", StringType, nullable = false)
    .add("link", StringType, nullable = false)

}
