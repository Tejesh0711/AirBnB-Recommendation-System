package hBase

import javax.inject.Singleton

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by vinay on 4/17/17.
  */

@Singleton
class AverageAnalysisOfListing {


  def getAverageAnalysisOfPriceByRoomType(place: String): Map[String, Double] = {
    val columnFamily: Array[Byte] = ListingsAnalysisByPlace.AveragePriceByRoomType.getBytes
    val homeColumn: Array[Byte] = AveragePriceByRoomType.EntireHomeApt.getBytes
    val sharedColumn: Array[Byte] = AveragePriceByRoomType.SharedRoom.getBytes
    val privateColumn: Array[Byte] = AveragePriceByRoomType.PrivateRoom.getBytes

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val table = connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))
    import org.apache.hadoop.hbase.util.Bytes
    val get = new Get(Bytes.toBytes(place))
    //get.getFamilyMap;
    get.addFamily(columnFamily)

    val result = table.get(get)

    val homeAveragePrice = Bytes.toDouble(result.getValue(columnFamily, homeColumn))
    val sharedAveragePrice = Bytes.toDouble(result.getValue(columnFamily, sharedColumn))
    val privateAveragePrice = Bytes.toDouble(result.getValue(columnFamily, privateColumn))

    val aggData = Map(AveragePriceByRoomType.SharedRoom -> sharedAveragePrice, AveragePriceByRoomType.PrivateRoom -> privateAveragePrice, AveragePriceByRoomType.EntireHomeApt -> homeAveragePrice)
    connection.close()
    table.close()
    aggData
  }


  def getAverageAnalysisOfPriceByNoOfRooms(place: String) = {
    val columnFamily: Array[Byte] = ListingsAnalysisByPlace.AveragePriceByNoOfRooms.getBytes
    val one: Array[Byte] = AveragePriceByNoOfRooms.one.getBytes
    val two: Array[Byte] = AveragePriceByNoOfRooms.two.getBytes
    val three: Array[Byte] = AveragePriceByNoOfRooms.three.getBytes
    val fourPlus: Array[Byte] = AveragePriceByNoOfRooms.fourPlus.getBytes

    //Get the Connection
    val connection = hBase.getConnection
    //Get the Table
    val tabel = connection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))
    import org.apache.hadoop.hbase.util.Bytes
    val get = new Get(Bytes.toBytes(place))
    //get.getFamilyMap;
    get.addFamily(columnFamily)

    val result = tabel.get(get)

    val oneAveragePrice: Array[Byte] = result.getValue(columnFamily, one)
    val twoAveragePrice: Array[Byte] = result.getValue(columnFamily, two)
    val threeAveragePrice: Array[Byte] = result.getValue(columnFamily, three)
    val fourPlusAveragePrice: Array[Byte] = result.getValue(columnFamily, fourPlus)
    val onePrice = Bytes.toDouble(oneAveragePrice)
    val twoPrice = Bytes.toDouble(twoAveragePrice)
    val threePrice = Bytes.toDouble(threeAveragePrice)
    val fourPrice = Bytes.toDouble(fourPlusAveragePrice)
    connection.close()
    tabel.close()
    val aggData = Map(AveragePriceByNoOfRooms.one -> onePrice, AveragePriceByNoOfRooms.two -> twoPrice, AveragePriceByNoOfRooms.three -> threePrice, AveragePriceByNoOfRooms.fourPlus -> fourPrice)
    aggData
  }

  def getCityTrend(city: String) = {
    val columnFamily: Array[Byte] = ListingsAnalysisByPlace.CityTrend.getBytes

    //Get the Table
    val table = hBase.getConnection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))

    val get = new Get(Bytes.toBytes(city)).addFamily(columnFamily)

    val result = table.get(get)


    val resultSet: java.util.NavigableMap[Array[Byte], Array[Byte]] = result.getFamilyMap(columnFamily)

    val keySet: java.util.Set[Array[Byte]] = resultSet.keySet()

    val iterator = keySet.iterator()

    // I really wish I could write Java streams here!!!! scala 2.11.7 doesn't support it.

    var map = Map[String, Float]()
    while (iterator.hasNext) {
      val nextQualifier = iterator.next()
      val key = nextQualifier
      val value = result.getValue(columnFamily, nextQualifier);
      map = map + (Bytes.toString(key) -> (100 - Bytes.toFloat(value)))
    }
    table.close()
    map
  }


  def getTopCustomers(city: String) = {
    val columnFamily: Array[Byte] = ListingsAnalysisByPlace.TopTwentyCustomers.getBytes

    //Get the Table
    val table = hBase.getConnection.getTable(TableName.valueOf(hBaseTableNames.ListingAnalysisByPlace))

    val get = new Get(Bytes.toBytes(city)).addFamily(columnFamily)

    val result = table.get(get)

    val resultSet: java.util.NavigableMap[Array[Byte], Array[Byte]] = result.getFamilyMap(columnFamily)

    val entrySet: java.util.Set[java.util.Map.Entry[Array[Byte], Array[Byte]]] = resultSet.entrySet()

    val iterator = entrySet.iterator()

    // I really wish I could write Java streams here!!!! scala 2.11.7 doesn't support it.

    var map = Map[String, Int]()
    while (iterator.hasNext) {
      val entry = iterator.next()

      map = map + (Bytes.toDouble(entry.getKey).toString -> Bytes.toInt(entry.getValue))
    }
    table.close()
    map
  }

  def getListingDetails(listingId: String) = {
    val columnFamily: Array[Byte] = "ListingData".getBytes

    //Get the Table
    val table = hBase.getConnection.getTable(TableName.valueOf(hBaseTableNames.ListingsData))

    val get = new Get(Bytes.toBytes(listingId.trim)).addFamily(columnFamily)

    Option(table.get(get).getFamilyMap(columnFamily)) match {
      case Some(x) => {
        val i = x.entrySet().iterator()
        var list = List[String](listingId)

        while (i.hasNext) {
          val e = i.next()
          if (Bytes.toString(e.getKey).equals("url")) {
            list = list :+ Bytes.toString(e.getValue)
          }

          if (Bytes.toString(e.getKey).equals("name")) {
            list = list :+ Bytes.toString(e.getValue)
          }


          if (Bytes.toString(e.getKey).equals("thumbnail")) {
            list = list :+ Bytes.toString(e.getValue)
          }

        }
        list
      }
      case _ => List()
    }
  }

}
