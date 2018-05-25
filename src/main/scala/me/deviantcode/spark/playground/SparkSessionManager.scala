package me.deviantcode.spark.playground

import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

trait SparkSessionManager {
  def logger: Logger

  def withLoanedSparkSession(sparkJob: SparkSession => Any): Unit = {
    val spark = Utils.getSparkSession(Some(4), Some(5))
    try {
      //run spark job
      sparkJob(spark)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        logger.error(s"${ex.getMessage}")
    } finally {
      spark.close()
    }
  }
}