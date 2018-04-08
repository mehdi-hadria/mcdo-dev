package mcdodata
/**
  * Created by mhadria on 04/04/2018.
  */
case class Transaction(transactionId : Long, transactionItemNumber : Int, orderNumber : Int, orderDate :String,
                       daypartId : Int, deliveryMethodId : Double, posAreaId : Double,soldMenuItemId : Int,
                       soldMenuEntreeQuantity: Int, soldMenuItemSideQuantity: Int, soldMenuItemBeverageQuantity : Int,
                       soldMenuItmDsrt : Int, estimatedGroupSize : Int, cmboPrensldMenuItmId: Int)

case class MenuItem(soldMenuItemId : Int, SoldMenuDesc : String, SoldMenuShortDesc : String)

case class PosArea(PosAreaCd : Int, PosAreaType : String, PosAreaId : Int )
