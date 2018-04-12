package mcdodata

import java.util.logging.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Created by mhadria on 11/04/2018.
  */
object DataAgreggator {
  def buildAnalyticalData(spark : SparkSession) = {

    /*import spark.implicits._*/
    val logger = Logger.getLogger(getClass().getEnclosingMethod().getName)

    // Read data from delivery method file
    logger.log(Level.INFO, "Reading data from file : Methode delivery", spark.sparkContext.appName)
    val dfDeliveryMethod = DataReader.readFile(spark,"deliveryMethod")
    logger.log(Level.INFO, "Data has been correctly loaded", spark.sparkContext.appName)

    // Read data from transactions file
    logger.log(Level.INFO, "Reading data from file : Transactions", spark.sparkContext.appName)
    val dfTransactions = DataReader.readFile(spark,"transaction")
    logger.log(Level.INFO, "Data has been correctly loaded", spark.sparkContext.appName)

    // Read data from Point of sales Area file
    logger.log(Level.INFO, "Reading data from file : PosArea", spark.sparkContext.appName)
    val dfPosArea = DataReader.readFile(spark,"posArea")
    logger.log(Level.INFO, "Data has been correctly loaded", spark.sparkContext.appName)

    // Create Udf function to use in new data frame creation
    logger.log(Level.INFO, "Dataframes transforming & aggregating ", spark.sparkContext.appName)
    val isWeekendUdf = udf(Utils.isWeekend _)
    import spark.implicits._
    val childTransaction = dfTransactions.
      // Get only transactions identified as a menu child
      filter(upper($"sld_menu_itm_na").contains("HAPPY MEAL"))
      // Deduplicate transactions to get only one transaction
      .distinct()
      // Rename column to avoid ambiguity
      .withColumnRenamed("pos_trn_id", "pos_trn_id_2")
      // Create additional column to tag transaction with a happy meal menu
      .withColumn("isHappyMeal", lit(1))
      // Select only the necessary data
      .select("pos_trn_id_2", "isHappyMeal")


    // Aggregate and enrich the final dataframe : have one transaction data with the necessary informations
    var aggDf = dfTransactions
    .join(dfPosArea, dfTransactions("terr_pos_area_cd") === dfPosArea("terr_pos_area_cd"))
    //.joinDayPart
    .join(dfDeliveryMethod, dfTransactions("terr_prd_dlvr_meth_cd") === dfDeliveryMethod("terr_prd_dlvr_meth_cd"))
    .join(childTransaction, dfTransactions("pos_trn_id") === childTransaction("pos_trn_id_2"), "left_outer")
    .withColumn("isWeekEnd", isWeekendUdf(dfTransactions("pos_ord_dt")))
    // Tag transactions with flag :  family(1) or not(0)
    .withColumn("isFamily", when($"isHappyMeal".isNull, 0).otherwise(1))
    // Tag transactions with flag Addon(1) or not (0)
    .withColumn("isAddOn", when($"SLD_MENU_ITM_SIDE_QT" > 0, 1).otherwise(0))
    .select("POS_TRN_ID", "POS_ORD_DT",  "ESTD_TRN_GEST_GRP_SIZE_QT",
            "CMBO_PREN_SLD_MENU_ITM_ID", "TERR_POS_AREA_DS",
            "TERR_PRD_DLVR_METH_DS", "isWeekEnd", "isFamily", "isAddOn")
    .distinct()

    // Rename columns using  the referential
    val renamedDf = aggDf.select(aggDf.columns.map(c => col(c).as(Referentials.ColumnNamesRef.getOrElse(c, c))): _*)
    logger.log(Level.INFO, "Dataframes transforming & aggregating has ended successfully", spark.sparkContext.appName)
    renamedDf


  }


}
