
name := "spark-playground"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "co.theasi" %% "plotly" % "0.2.0",
  "graphframes" % "graphframes" % "0.5.0-spark2.1-s_2.11"


  //  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.2.jre8",
//  "com.rockymadden.stringmetric" %% "stringmetric-core" % "0.27.4",
//  "com.lucidworks.spark" % "spark-solr" % "3.4.0"
)

testOptions += Tests.Setup(_ => sys.props("testMode") = "true")
testOptions += Tests.Cleanup(_ => sys.props.remove("testMode"))

