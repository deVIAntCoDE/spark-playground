package me.deviantcode.spark.playground

import org.apache.spark.sql.SparkSession

object Utils {

  /*
  Creating spark sessions
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
}
