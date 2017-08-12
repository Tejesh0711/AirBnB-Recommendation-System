package actors

import actors.ConsumerActor.{ConsumedRecords, ReadDataFromKafka}
import actors.KafkaConsumerClientManagerActor.{GetRecommendation, RecommendedListing}
import akka.actor.{Actor, ActorRef, Props}
import kafkaClients.{KafkaRecommendationResponseConsumer, KafkaRecommendationResultProducer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords}

import scala.collection.mutable

/**
  * Created by akashnagesh on 4/13/17.
  */

object KafkaConsumerClientManagerActor {

  case class GetRecommendation(userId: String)

  case class RecommendedListing(values: Iterable[ConsumerRecord[String, String]])

}

class KafkaConsumerClientManagerActor(consumer: ActorRef, myKafkaResultProducer: KafkaRecommendationResultProducer) extends Actor {

  override def preStart() = {
    println("--------------------------in prestart!!!")
    consumer ! ReadDataFromKafka()
    context.actorOf(Props(classOf[SparkStreamingListnerActor])) ! SparkStreamingListnerActor.StartListeningToKafka(myKafkaResultProducer)
  }

  var bufferMap = new mutable.HashMap[String, ActorRef]()

  override def receive = {
    case GetRecommendation(userId) => {
      println("inside get recommendation")
      println("this is the web socket that sent message" + sender())
      bufferMap.put(userId, sender())
    }
    case RecommendedListing(values) => {
      for (value <- values) yield {
        println()
        bufferMap.get(value.key()).foreach(x => {
          println("sending value to " + x);
          x ! value.value()
          bufferMap.remove(value.key())
        })

      }
    }
  }
}


object ConsumerActor {

  case class ReadDataFromKafka()

  case class ConsumedRecords(records: ConsumerRecords[String, String])

}

class ConsumerActor(consumerClient: KafkaRecommendationResponseConsumer) extends Actor {

  var managerActor: ActorRef = null

  override def receive = {
    case ReadDataFromKafka() => {
      managerActor = sender()
      println("reading data from kafka")
      new Thread(new ConsumerThread(self, consumerClient),"Kafka_consumer_thread").start()
    }

    case ConsumedRecords(records) => {
      import scala.collection.JavaConverters._
      managerActor ! RecommendedListing(records.asInstanceOf[ConsumerRecords[String, String]].asScala)
    }
  }
}


