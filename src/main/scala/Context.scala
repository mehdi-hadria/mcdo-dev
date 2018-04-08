import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by mhadria on 04/04/2018.
  */
trait Context {
  lazy val sparkConf = new SparkConf()
    .setAppName("Learn Spark")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")

  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

}
