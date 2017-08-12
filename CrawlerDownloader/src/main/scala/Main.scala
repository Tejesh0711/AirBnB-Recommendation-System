import actors.Supervisor
import actors.Supervisor.StartDownload
import akka.actor.{ActorSystem, Props}

/**
  * Created by akashnagesh on 4/14/17.
  */
object Main extends App {
  val system = ActorSystem()

  system.actorOf(Props(new Supervisor)) ! StartDownload("http://insideairbnb.com/get-the-data.html", "/home/vinay/InsideAirbnb/")

}


