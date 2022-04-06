package dive.address

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.mongodb.spark._
import com.mongodb.spark.config._

//case class Coin(address: String, spentTxid: String)

object SparkTestApp {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .appName("SparkTestApp")
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(args(0))
    df.createTempView("surveys")
    val result = spark.sql("""
	      select * from surveys
	  """)
    result.show()

    val df1 = MongoSpark.load[Coin](spark, ReadConfig(
      Map("uri" -> "mongodb://127.0.0.1/bitcore.coins")
    ))
    df1.printSchema()
    df1.show(50)

    df1.createTempView("coinsView")

    spark.sql("""
	      select * from coinsView
	  """)
      .show(10)

//    MongoSpark.save(result.write.options(
//      Map("uri" -> "mongodb://127.0.0.1/bitcore.coins_bak")
//    ).mode("append"))
    result.write.format("mongo")
      .option("uri","mongodb://127.0.0.1")
      .option("database", "bitcore")
      .option("collection", "coins_bak")
      .mode("append")
      .save()

    MongoSpark.load[Coin](spark, ReadConfig(
      Map("uri" -> "mongodb://127.0.0.1/bitcore.coins_bak")
    )).show()
    spark.stop()
  }

}
