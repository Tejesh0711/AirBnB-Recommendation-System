package actors

import actors.SparkStreamingListnerActor.StartListeningToKafka
import akka.actor.Actor
import kafkaClients.KafkaRecommendationResultProducer
import spark.StreamingClient

/**
  * Created by akashnagesh on 4/18/17.
  */

object SparkStreamingListnerActor {

  case class StartListeningToKafka(myKafkaResultProducer: KafkaRecommendationResultProducer)

}

class SparkStreamingListnerActor extends Actor {
  override def receive: Receive = {
    case StartListeningToKafka(myKafkaResultProducer) => {
      println("++++++++++++++++++==========++++===+++++==++==+=++====+=+=++=++==+starting to listening to kafka")
      val sparkClient = new StreamingClient(myKafkaResultProducer)
    }
  }
}
