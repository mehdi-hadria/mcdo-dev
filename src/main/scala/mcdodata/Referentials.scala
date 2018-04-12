package mcdodata

/**
  * Created by mhadria on 12/04/2018.
  */
object Referentials {

  // Referential to rename dataframe using significant names
  val ColumnNamesRef = Map (
    "POS_TRN_ID" -> "transactionId",
    "POS_ORD_DT" -> "orderDate",
    "ESTD_TRN_GEST_GRP_SIZE_QT" -> "groupSize",
    "CMBO_PREN_SLD_MENU_ITM_ID" -> "itemParentId",
    "TERR_POS_AREA_DS" -> "PosArea" ,
    "TERR_PRD_DLVR_METH_DS" -> "deliveryMethod"
  )

}
