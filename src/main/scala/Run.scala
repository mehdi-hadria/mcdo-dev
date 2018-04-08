import mcdodata.DataReader._
import mcdodata.Utils._
/**
  * Created by mhadria on 04/04/2018.
  */
object Run extends Context {
  def main(args : Array[String]): Unit ={
    import org.apache.spark.sql.functions.udf
    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.0")
    val dfMenuItem = ReadMenuItemsData(sparkSession)
    val dfTransactions = ReadTransactionData(sparkSession)
    val dfPosArea = ReadPosAreaData(sparkSession)
    val isWeekendUdf = udf(isWeekend _)
    dfTransactions.printSchema()
    dfMenuItem.printSchema()
    dfPosArea.printSchema()
    dfTransactions
    .join(dfMenuItem,dfTransactions("soldMenuItemId") === dfMenuItem("soldMenuItemId"))
    .join(dfPosArea,dfTransactions("posAreaId") === dfPosArea("posAreaId"))
    .show(10)










  }
}
