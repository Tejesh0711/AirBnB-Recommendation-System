package controllers

import javax.inject.Inject

import actors.{ConsumerActor, KafkaConsumerClientManagerActor, LoginActor, RecommendationWebSocketActor}
import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.routing.RoundRobinPool
import akka.stream.Materializer
import akka.util.Timeout
import dataAccessLayer.{UserActionMessages, UserDalImpl}
import hBase.AverageAnalysisOfListing
import kafkaClients.{KafkaRecommendationRequestProducer, KafkaRecommendationResponseConsumer, KafkaRecommendationResultProducer}
import models.{FormsData, User}
import org.apache.commons.lang3.StringUtils
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.libs.json.JsValue
import play.api.libs.streams.ActorFlow
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
class HomeController @Inject()(val messagesApi: MessagesApi)(userDalImpl: UserDalImpl)(averageAnalysisOfListing: AverageAnalysisOfListing)
                              (myKafkaProducer: KafkaRecommendationRequestProducer)(myKafkaConsumer: KafkaRecommendationResponseConsumer)(myKafkaResultProducer: KafkaRecommendationResultProducer)
                              (implicit ec: ExecutionContext, system: ActorSystem, materializer: Materializer) extends Controller with I18nSupport {

  /**
    * Create an Action to render an HTML page with a welcome message.
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `GET` request with
    * a path of `/`.
    */

  val logger = play.Logger.of("airbnb_logger")

  val loginRouter = system.actorOf(Props(classOf[LoginActor], userDalImpl).withRouter(RoundRobinPool(10)), name = "LoginActor")
  val consumerActor = system.actorOf(Props(classOf[ConsumerActor], myKafkaConsumer))
  val consumerClientManagerActor = system.actorOf(Props(classOf[KafkaConsumerClientManagerActor], consumerActor, myKafkaResultProducer))


  def index = Action {
    logger.info("User in landing page")
    Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)(StringUtils.EMPTY))
  }

  def designStrategy = Action {
    logger.info("User accessing design strategies")
    Ok(views.html.designStrategies())
  }

  def userLogin = Action.async {
    implicit request =>
      implicit val timeout: Timeout = Timeout(2 seconds)
      FormsData.userForm.bindFromRequest().fold(
        formWithErrors => Future.successful(BadRequest),
        userTuple => {
          loginRouter ? LoginActor.GetUser(userTuple._1, userTuple._2)
        } map {
          case Some(user) => Ok(views.html.loggedInPage("Welcome User")).withSession("user" -> userTuple._1)
          case None => Ok("Invalid credentials")
        }
      )
  }

  def createUser = Action.async {
    implicit request =>
      implicit val timeout: Timeout = Timeout(2 seconds)
      FormsData.createUserForm.bindFromRequest().fold(
        errorForm => Future.successful(Ok),
        user => {
          val user1 = User(0, user.name, user.age, user.email, user.password)
          loginRouter ? LoginActor.CreateUser(user1)
        } map (someMes => someMes match {
          case UserActionMessages.emailAlreadyExists => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("Email Id Already Exists"))
          case UserActionMessages.genericError => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("Unable to create user. Please try again."))
          case _ => Ok(views.html.index(FormsData.userForm)(FormsData.createUserForm)("User account created! Login to use our service"))
        })
      )

  }


  def getRecommendation = WebSocket.acceptOrResult[JsValue, JsValue] { request =>
    Future.successful(request.session.get("user") match {
      case None => Left(Forbidden)
      case Some(user) => Right(ActorFlow.actorRef(out => RecommendationWebSocketActor.props(out, myKafkaProducer, consumerClientManagerActor, user,averageAnalysisOfListing)))
    })
  }

}
