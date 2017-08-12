package controllers

import javax.inject.{Inject, Singleton}

import hBase.AverageAnalysisOfListing
import play.api.libs.json._
import play.api.mvc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class UserController @Inject()(averageAnalysisOfListing: AverageAnalysisOfListing) extends Controller {


  def graph1 = Action.async {
    Future(averageAnalysisOfListing.getAverageAnalysisOfPriceByRoomType("Berlin")) map (x => Ok(Json.toJson(x)))
  }


  def graph2 = Action.async {
    Future(averageAnalysisOfListing.getAverageAnalysisOfPriceByNoOfRooms("Berlin")) map (x => Ok(Json.toJson(x)))
  }

  def graph3 = Action.async {
    Future(averageAnalysisOfListing.getCityTrend("Boston")) map (x => Ok(Json.toJson(x)))
  }

  def graph4 = Action.async {
    Future(averageAnalysisOfListing.getTopCustomers("Boston")) map (x => Ok(Json.toJson(x)))
  }

}
