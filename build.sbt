name := "mcdo"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.0" ,
    "org.apache.spark" %% "spark-sql" % "2.3.0",
    "com.typesafe" % "config" % "1.2.1"
  )

dbcApiUrl := "https://northeurope.azuredatabricks.net/api/1.2"
dbcUsername := "mehdi.hadria@ekimetrics.com"
dbcPassword := "MehAd@Eki042018O"
dbcClusters += "TestCluster"
