package actors


import actors.Downloaders.DownloadFile
import actors.Supervisor.{DownloadComplete, DownloadFailed, StartDownload}
import akka.actor.{Actor, PoisonPill}
import org.apache.commons.validator.routines.UrlValidator
import org.jsoup.Jsoup

/**
  * Created by akashnagesh on 4/14/17.
  */

object Supervisor {

  case class StartDownload(url: String, pathOnFs: String) {
    require(url != null)
    require(pathOnFs != null)
  }

  case class DownloadFailed(url: String, pathOnFs: String)

  case class DownloadComplete()

}

class Supervisor extends Actor {

  val maxRetries = 3
  var urlAndRetriesCount = Map.empty[String, Int]
  var numberOfLinks = 0

  var downloaded = 0

  override def receive: Receive = {
    case StartDownload(url, pathOnFs) => {
      println("starting====")
      parse(url).filter(isItWorthToDownload _).map(url => {
        Thread.sleep(2000)
        numberOfLinks = numberOfLinks + 1
        context.actorOf(Downloaders.props) ! DownloadFile(url, pathOnFs)
      })
    }
    case DownloadFailed(url, pathOnFs) => {
      val noOfAttempts = urlAndRetriesCount.get(url).getOrElse(0)

      if (noOfAttempts > maxRetries) {
        downloaded = downloaded + 1
        sender() ! PoisonPill
        println("could not download from link " + url)
      }
      else {
        urlAndRetriesCount += (url -> (urlAndRetriesCount.getOrElse(url, 0) + 1))
        sender() ! DownloadFile(url, pathOnFs)
      }
    }

    case DownloadComplete() => {
      sender() ! PoisonPill
      downloaded = downloaded + 1
      println(downloaded)
      println(numberOfLinks)
      if (downloaded == numberOfLinks) {
        println("Download complete")
        context.system.terminate()
      }
    }
  }

  def parse(url: String): List[String] = {
    import scala.collection.JavaConverters._

    val response = Jsoup.connect(url)
      .ignoreContentType(true)
      .userAgent("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1")
      .execute()

    val contentType: String = response.contentType

    if (contentType.startsWith("text/html")) {
      val urlValidator = new UrlValidator()
      val doc = response.parse()
      doc.getElementsByTag("a").asScala.map(e => e.attr("href")).filter(link => urlValidator.isValid(link)).toList
    }
    else {
      List.empty
    }
  }

  def isItWorthToDownload(url: String) = url.contains("listings.csv") || url.contains("calendar.csv") || url.contains("reviews.csv") || url.contains("neighbourhoods.csv") || url.contains("neighbourhoods.geojson")

}
