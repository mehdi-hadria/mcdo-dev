package mcdodata

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
/**
  * Created by mhadria on 04/04/2018.
  */


  object DataReader {
    // Read data from transaction file
    def ReadTransactionData(spark : SparkSession) = {
      import spark.implicits._
      // Getting file properties
      val transactionPath = ConfigFactory.load().getString("dataSrcProperties.transaction.filePath")
      val transactionDelimiter = ConfigFactory.load().getString("dataSrcProperties.transaction.columnDelimiter").charAt(0)
      val transactionColumnNumber = ConfigFactory.load().getString("dataSrcProperties.transaction.columnNumber").toInt
      // Reading file
      val transactionFile = spark.sparkContext.textFile(transactionPath)
      val transactionHeader =  transactionFile.first()
      val dfTransactions = transactionFile.filter(line => line != transactionHeader)
                          .map(line => line.split(transactionDelimiter))
                          .filter(line => line.length == transactionColumnNumber)
                          .map(line => Transaction(line(0).trim().toLong,line(1).trim().toInt,line(2).trim().toInt,line(4).toString
                            ,line(29).trim().toInt,line(32).trim().toDouble,line(33).trim().toInt,line(35).trim().toInt
                            ,line(38).trim().toInt,line(39).trim().toInt,line(40).trim().toInt,line(41).trim().toInt
                            ,line(43).trim().toInt,line(36).trim().toInt))
                          .toDF()
      dfTransactions
    }
    // Read data from Menu items file
    def ReadMenuItemsData(spark : SparkSession) = {
      import spark.implicits._
      val menuItemPath = ConfigFactory.load().getString("dataSrcProperties.menuItems.filePath")
      val menuItemDelimiter = ConfigFactory.load().getString("dataSrcProperties.menuItems.columnDelimiter")
      val menuItemColumnNumber = ConfigFactory.load().getString("dataSrcProperties.menuItems.columnNumber").toInt
      val menuItemFile = spark.sparkContext.textFile(menuItemPath)
      val menuItemHeader = menuItemFile.first()
      val dfMenuItem = menuItemFile.filter(line => line != menuItemHeader)
                      .map(line => line.split(menuItemDelimiter))
                      .map(line => MenuItem(line(1).toInt,line(2).toString,line(3).toString))
                      .toDF()
      dfMenuItem
    }
    // Read data from  pos area file
    def ReadPosAreaData(spark : SparkSession) = {
      import spark.implicits._
      val posAreaPath = ConfigFactory.load().getString("dataSrcProperties.posArea.filePath")
      val posAreaDelimiter = ConfigFactory.load().getString("dataSrcProperties.posArea.columnDelimiter")
      val posAreaColumnNumber = ConfigFactory.load().getString("dataSrcProperties.posArea.columnNumber").toInt

      val posAreaFile = spark.sparkContext.textFile(posAreaPath)
      val posAreaHeader = posAreaFile.first()
      val dfPosArea = posAreaFile.filter(line => line != posAreaHeader)
                      .map(line => line.split(posAreaDelimiter))
                      .map(line => PosArea(line(0).trim().toInt,line(2).toString,line(3).trim().toInt))
                      .toDF()
      dfPosArea
    }


  }

