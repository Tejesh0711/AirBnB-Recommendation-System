package spark

import hBase.{ListingsData, hBase, hBaseTableNames}
import kafkaClients.KafkaRecommendationResultProducer
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
  * Created by akashnagesh on 4/17/17.
  */
class StreamingClient(kafkaProducer: KafkaRecommendationResultProducer) {

  //  val conf = Play.current.configuration
  val bootStrapServer = "localhost:9092" //conf.getString("kafka.bootstrap.servers").getOrElse("no bootstrap server in app config")
  println("inside this classss +++++++++++++++++++++" + bootStrapServer)

  //val userPreferenceTopic = "userPreference2" //conf.getString("kafka.topicIn").getOrElse("no input topic")
  // val sparkAppName = "airbnb"
  //conf.getString("spark.appName").getOrElse("no spark application name")
  // val master = "local[*]"
  //conf.getString("spark.master").getOrElse("no spark master")
  val consumerGroupId = "sparkConsumer" //conf.getString("streamconsumer.groupid").getOrElse("no group id for consumer")

  val kafkaParams = Map[String, Object](ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootStrapServer,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.GROUP_ID_CONFIG -> consumerGroupId,
    "auto.offset.reset" -> "latest")

  val sc = SparkCommons.sc

  val ssc = SparkCommons.streamingContext
  //val sparkConfig = new SparkConf(false).setMaster(master).setAppName(sparkAppName).set("spark.driver.host", "localhost")

  //val sc = new SparkContext(sparkConfig)

  val listingPredictor = new ListingPredictor(sc)

  //val ssc = new StreamingContext(sc, Milliseconds(4000))

  val kafkaReceiverParams = Map[String, String](
    "metadata.broker.list" -> "192.168.10.2:9092")

  val topics = Array("userPreference3")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  val mapped = stream.map(record => (record.key(), record.value()))

  println("---------------------------")
  val x = mapped.foreachRDD { x => {
    val l = x.collect()
    l.foreach { individualRecord =>
      val ar = individualRecord._2.toCharArray match {
        case Array(a, b, c) => (a, b, c)
      }

      implicit def charToDouble(c: Char) = c.toDouble - 48

      println("________________---------__-" + charToDouble(ar._1), charToDouble(ar._2), charToDouble(ar._3))
      val messageToPublish = listingPredictor.recommendListing(charToDouble(ar._1), charToDouble(ar._2), charToDouble(ar._3)).map(a => a.toString())

      println("recommended listings ======" + messageToPublish)
      kafkaProducer.publishMessage(individualRecord._1, messageToPublish.toString())

    }
  }
  }
  ssc.start()
  ssc.awaitTermination()
}