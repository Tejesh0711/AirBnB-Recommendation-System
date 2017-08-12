package actors

import java.io._
import java.net.{HttpURLConnection, URL}

import actors.Downloaders.DownloadFile
import actors.Supervisor.{DownloadComplete, DownloadFailed}
import akka.actor.{Actor, Props}
import org.jsoup.Jsoup

import scala.util.Try

/**
  * Created by akashnagesh on 4/14/17.
  */

object Downloaders {

  def props = Props(new Downloaders)

  case class DownloadFile(url: String, pathOnFs: String)

}

class Downloaders extends Actor {
  override def receive: Receive = {
    case DownloadFile(url, pathOnFs) => {
      println("Downloading from: " + url)
      Try(downloadFile(url, pathOnFs)).map(x => sender() ! DownloadComplete())
        .getOrElse(sender() ! DownloadFailed(url, pathOnFs))
    }
  }

  def downloadFile(urlToDownload: String, pathOnFs: String) = {
    val conn = new URL(urlToDownload).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    val in: InputStream = conn.getInputStream
    val name = urlToDownload.split("/")
    val folder = this.createDirectoryIfAbsent(pathOnFs, (name(name.size - 1).split("\\.")(0)))
    val fileToDownloadAs = new java.io.File(folder.getAbsolutePath + File.separator + System.currentTimeMillis() + name(name.size - 1))
    val out: OutputStream = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
    val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
    out.write(byteArray)
    out.flush()
    in.close()
    out.close()
    true
  }

  def downloadFile1(urlToDownload: String, pathOnFs: String) = {
    val resultImageResponse = Jsoup.connect(urlToDownload)
      .ignoreContentType(true).execute();
    import java.io.FileOutputStream
    val name = urlToDownload.split("/")
    val folder = this.createDirectoryIfAbsent(pathOnFs, (name(name.size - 1).split("\\.")(0)))
    val out = new FileOutputStream(new File(folder + File.separator + System.currentTimeMillis() + name(name.size - 1)))
    out.write(resultImageResponse.bodyAsBytes) // resultImageResponse.body() is where the image's contents are.
  }

  def createDirectoryIfAbsent(pathOnFs: String, name: String): File = {
    val folder = new File(pathOnFs + File.separator + name)
    if (!folder.exists()) {
      folder.mkdir()
    }
    folder
  }

}
