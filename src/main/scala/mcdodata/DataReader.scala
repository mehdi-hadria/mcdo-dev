package mcdodata

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

/**
  * Created by mhadria on 04/04/2018.
  */


  object DataReader {


  def readFile(spark : SparkSession,file : String) = {
      val filePath = ConfigFactory.load().getString("dataSrcProperties."+file+".filePath")
      val fileDelimiter = ConfigFactory.load().getString("dataSrcProperties."+file+".columnDelimiter")
      val fileColumsToSelect = ConfigFactory.load().getStringList("dataSrcProperties."+file+".columnsList").asScala.toList
      spark
      .read
      .option("header","true")
      .option("inferSchema","true")
      .option("delimiter",fileDelimiter)
      .csv(filePath)
      .toDF()
      .select(fileColumsToSelect.map(col): _*)

    }

  }

