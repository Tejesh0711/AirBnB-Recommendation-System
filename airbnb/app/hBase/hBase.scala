package hBase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * Created by vinay on 4/16/17.
  */

object hBase {


  import org.apache.hadoop.hbase.client.ConnectionFactory


  //Return Connection pool for an HBase instance

  def getConnection = {
    val conf: Configuration = HBaseConfiguration.create
    ConnectionFactory.createConnection(conf)
  }

}
