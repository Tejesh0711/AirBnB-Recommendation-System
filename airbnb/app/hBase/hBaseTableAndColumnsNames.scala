package hBase


object hBaseTableNames {
  //HBase Table for storing all Listing Analysis By Place
  val ListingAnalysisByPlace = "ListingsAnalysisByPlace"
  //Coulum Family for the HBase table ListingsAnalysisByPlace
  val ListingsData = "ListingsData"
}


//Class has all column families for the ListingsAnalysisByPlace table
object ListingsAnalysisByPlace {

  val AveragePriceByRoomType = "AveragePriceByRoomType"


  val AveragePriceByNoOfRooms = "AveragePriceByNoOfRooms"

  val CityTrend = "CityTrend"

  val TopTwentyCustomers = "TopTenCustomers"

}

//Class has all the column families for ListingsDataTable
object ListingsData {
  val ListingData = "ListingData"
  val SentimentData = "PositiveSentiment"
}


//Class has all columns for AveragePriceByRoomType column family
object AveragePriceByRoomType {

  val EntireHomeApt = "Entire home/apt"
  val SharedRoom = "Shared room"
  val PrivateRoom = "Private room"

}

//Class has all columns for AveragePriceByNoOfRooms column family
object AveragePriceByNoOfRooms {
  val one = "one"
  val two = "two"
  val three = "three"
  val fourPlus = "fourPlus"
}
