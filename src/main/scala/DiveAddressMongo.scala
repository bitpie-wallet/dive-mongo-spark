package dive.address

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.sql.fieldTypes.ObjectId
import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.functions.{col, count}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class Coin(_id: String, address: String, spentTxid: String)
case class Tx(_id: ObjectId, spentTxid: String, status: Integer)
case class Address(_id: ObjectId, address: String, status: Integer)


object DiveAddressMongo {
  val mongoUri = "mongodb://127.0.0.1"
  val database = "bitcore"
  val coinsUri = mongoUri + "/" + database + "." + "coins"
  val txsCollection = "dive_txs"
  val txsCollectionUri = mongoUri + "/" + database + "." + txsCollection
  val addressesCollection = "dive_addresses"
  val addressesCollectionUri =  mongoUri + "/" + database + "." + addressesCollection
  def main(args: Array[String]) = {
    if(args.length == 2){
      val command = args(0)
      val value = args(1)
      val sparkSession = SparkSession.builder()
        .appName("SparkTestApp")
        .getOrCreate()
//      sparkSession.sqlContext.setConf("spark.sql.shuffle.partitions","8")
      val dfCoins = MongoSpark.load[Coin](sparkSession, ReadConfig(
        Map("uri" -> coinsUri)
      ))
//      dfCoins.printSchema()
//      dfCoins.show(10)
//      dfCoins.select("*").limit(1).show()
      dfCoins.createOrReplaceTempView("coins_tmp_view")
      val dfTxs = MongoSpark.load[Tx](sparkSession, ReadConfig(
        Map("uri" -> txsCollectionUri)
      ))
      dfTxs.createOrReplaceTempView("txs_tmp_view")
      val dfAddresses = MongoSpark.load[Address](sparkSession, ReadConfig(
        Map("uri" -> addressesCollectionUri)
      ))
      dfAddresses.createOrReplaceTempView("addresses_tmp_view")

      import sparkSession.implicits._

      def diveAddressByTxs():Unit={
        val unDiveTxExist = dfTxs.select("*").where("status = 0 and spentTxid <> ''").limit(1)
        unDiveTxExist.show(1)
        if(unDiveTxExist.count() == 1){
          val unDiveTxs = dfTxs.select("_id", "spentTxid", "status").where("status = 0  ")
          unDiveTxs.as[Tx].filter(tx => !tx.spentTxid.equals("") ).createOrReplaceTempView("un_dive_txs_tmp_view")
//          unDiveTxs.filter(col("spentTxid"))
//          unDiveTxs.createOrReplaceTempView("un_dive_txs_tmp_view")
          sparkSession.sql(
            """
              |SELECT  coins_tmp_view.address, 0
              |FROM un_dive_txs_tmp_view inner join coins_tmp_view on un_dive_txs_tmp_view.spentTxid = coins_tmp_view.spentTxid
              |WHERE coins_tmp_view.address is not null
              |""".stripMargin)
            .toDF("address","status")
            .distinct().createOrReplaceTempView("dived_addresses_tmp_view")
          sparkSession.sql(
            """
              |SELECT dived_addresses_tmp_view.address, dived_addresses_tmp_view.status
              |FROM dived_addresses_tmp_view
              |WHERE not EXISTS (
              |   SELECT 1 FROM addresses_tmp_view WHERE dived_addresses_tmp_view.address = addresses_tmp_view.address
              |)
              |""".stripMargin)
            .toDF("address","status")
            .write
            .format("mongo")
            .option("uri", mongoUri)
            .option("database", database)
            .option("collection", addressesCollection)
            .mode("append")
            .save()
          unDiveTxs.as[Tx].map(tx=>Tx(tx._id, tx.spentTxid, 1)).write.format("mongo")
            .option("uri", mongoUri)
            .option("database", database)
            .option("collection", txsCollection)
            .mode("append")
            .save()
          diveTxByAddresses()
        }
      }
      def diveTxByAddresses():Unit={
        import sparkSession.implicits._
        val unDiveAddressExist = dfAddresses.select("*").where("status = 0").limit(1)
        if(unDiveAddressExist.count() == 1){
          val unDiveAddresses = dfAddresses.select("_id", "address", "status").where("status == 0")
          unDiveAddresses.createOrReplaceTempView("un_dive_address_tmp_view")
          sparkSession.sql(
            """
              |SELECT  coins_tmp_view.spentTxid, 0
              |FROM un_dive_address_tmp_view inner join coins_tmp_view on un_dive_address_tmp_view.address = coins_tmp_view.address
              |WHERE coins_tmp_view.spentTxid is not null
              |""".stripMargin)
            .toDF("spentTxid","status")
            .distinct().createOrReplaceTempView("dived_txs_tmp_view")
          sparkSession.sql(
            """
              |SELECT dived_txs_tmp_view.spentTxid, dived_txs_tmp_view.status
              |FROM dived_txs_tmp_view
              |WHERE not EXISTS (
              |   SELECT 1 FROM txs_tmp_view WHERE dived_txs_tmp_view.spentTxid = txs_tmp_view.spentTxid
              |)
              |""".stripMargin)
            .toDF("spentTxid","status")
            .write
            .format("mongo")
            .option("uri", mongoUri)
            .option("database", database)
            .option("collection", txsCollection)
            .mode("append")
            .save()
          unDiveAddresses.as[Address].map(ad=>Address(ad._id, ad.address, 1)).write.format("mongo")
            .option("uri", mongoUri)
            .option("database", database)
            .option("collection", addressesCollection)
            .mode("append")
            .save()
        }
        diveAddressByTxs()
      }


      if("dive".equals(command)){
        import sparkSession.implicits._
        val dfAddressInit = sparkSession.createDataset(Seq((value, 0))).toDF("address","status")
        dfAddressInit.write.format("mongo")
          .option("uri", mongoUri)
          .option("database", database)
          .option("collection", addressesCollection)
          .mode("append")
          .save()
        diveTxByAddresses()


//        case class AddressInit(address: String, status: Integer)
//        val data = Seq(AddressInit(value, 0))
//        val dfAddressInit = data.toDF("address", "status")
//        val dfAddressInit = sparkSession.createDataFrame(Seq((value, 0)))
//        val schema = StructType(List(StructField("address", StringType, nullable = false),StructField("status", IntegerType, nullable = false)))
//        val addressInitRdd = sparkSession.sparkContext.parallelize(Seq(Row(value,0)))
//        val dfAddressInit = sparkSession.createDataFrame(addressInitRdd, schema)
      }
      if("restart".equals(command)){
        diveTxByAddresses()
      }
      sparkSession.stop()
    }
  }



}
