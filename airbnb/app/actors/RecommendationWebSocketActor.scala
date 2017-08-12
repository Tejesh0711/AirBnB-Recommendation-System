package actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import hBase.AverageAnalysisOfListing
import kafkaClients.KafkaRecommendationRequestProducer
import play.api.libs.json.{JsValue, Json}

/**
  * Created by akashnagesh on 4/11/17.
  */
object RecommendationWebSocketActor {
  def props(out: ActorRef, kafkaProducer: KafkaRecommendationRequestProducer,
            kafkaClientManagerActor: ActorRef, user: String, averageAnalysisOfListing: AverageAnalysisOfListing) =
    Props(new RecommendationWebSocketActor(out, kafkaProducer, kafkaClientManagerActor, user, averageAnalysisOfListing))
}

class RecommendationWebSocketActor(val out: ActorRef, val kafkaProducer: KafkaRecommendationRequestProducer,
                                   val kafkaClientManagerActor: ActorRef, val user: String, val averageAnalysisOfListing: AverageAnalysisOfListing) extends Actor {

  def receive = {
    case msg: JsValue => {

      println("this is a web socket actor ref + " + self)
      println("inside actor default receive")

      kafkaProducer.publishMessage(user, msg.toString())
      kafkaClientManagerActor ! KafkaConsumerClientManagerActor.GetRecommendation(user)
      // out ! msg + "appending this from server"
      context.become(onConsumerMessageBehavior)
    }
  }

  def onConsumerMessageBehavior: Receive = {
    case msg: String => {
      println("final msg in actor")
      val recommendedListing = msg.substring(5, msg.length - 1).split(",").map(l => averageAnalysisOfListing.getListingDetails(l)).filter(l => (l.size == 4) && (l(2).length > 5))
      recommendedListing.foreach(println _)
      out ! Json.toJson(recommendedListing)
      self ! PoisonPill
    }
  }
}

