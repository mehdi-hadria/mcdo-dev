package mcdodata

import java.text.SimpleDateFormat
import java.util.Calendar
/**
  * Created by mhadria on 06/04/2018.
  */
object Utils {

  // Check if "day" is week end day or not
  def isWeekend(day : String) = {
      val dateFormat = new SimpleDateFormat("dd-MM-yyyy")
      val calendar = Calendar.getInstance()
      var dayDate = dateFormat.parse(day)
      calendar.setTime(dayDate)
      if(calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
        {
          true
        }
      else false
  }


}
