import mcdodata.DataAgreggator

/**
  * Created by mhadria on 04/04/2018.
  */
object Run extends Context {
  def main(args : Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0")

    try {

      var dataset = DataAgreggator.buildAnalyticalData(sparkSession)
      dataset.printSchema()
      dataset.show()
      //3652
    }
    catch{
      case ex : Exception => println(ex.getMessage)
    }









  }
}
