package me.deviantcode.spark.playground

import org.apache.spark.sql.SparkSession

object Utils {

  /*
  Creating spark sessions
  give it optional values for partitions and shuffles to indicate local mode runs
   */
  def getSparkSession(par: Option[Int] = None, shuffleParts: Option[Int] = None): SparkSession = {
    val spark = par.map(cores => {
      SparkSession
        .builder()
        .appName("spark-playground")
        .master(s"local[$cores]")
        .getOrCreate()
    }).getOrElse(
      SparkSession
        .builder()
        .appName("spark-playground")
        .getOrCreate()
    )
    //set new runtime options
    shuffleParts.foreach(spark.conf.set("spark.sql.shuffle.partitions", _)) // reduce shuffle partitions to reduce network overhead in localmode
    spark
  }

  //calculate minuted from midnight, input is military time format
  def getMinuteOfDay(depTime: String): Int = (depTime.toInt / 100).toInt * 60 + (depTime.toInt % 100)

}
