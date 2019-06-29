package main.scala

import org.apache.spark.{SparkConf, SparkContext}
import main.scala.MergeUtils._
import org.apache.spark.sql._
import org.apache.log4j._

object CountCSVFileRecords {

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val appName = args(0)
    val sparkMaster = args(1)
    val csvDelimiter = args(2)
    val csvFile = args(3)

    //set up the spark configuration
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)

    // Use new SparkSession interface in Spark 2.0
    val sparkSession = SparkSession
      .builder
      .config(sparkConf)
      .getOrCreate()

    println("Application Id".padTo(30,' ')+ " : " + sparkSession.sparkContext.applicationId)
    printCurrentTime("Start " + appName + " Program",30)
    // Load CSV load file
    printCurrentTime("Start of Loading CSV File",30)
    val csvDF = loadCSV(sparkSession,csvDelimiter,csvFile)
    printCurrentTime("End of Loading CSV File",30)
    println("CSV File Record Count".padTo(30,' ')+ " : " + csvDF.count())
    printCurrentTime("End CSV Record Count Program",30)
    printCurrentTime("End " + appName + " Program",30)
  }
}
