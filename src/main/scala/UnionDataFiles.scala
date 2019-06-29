package main.scala

import main.scala.MergeUtils._
import org.apache.spark.sql._
import org.apache.log4j._

object UnionDataFiles {

  def unionDataFrames(masterDF: DataFrame, deltaDF: DataFrame): DataFrame = {

    val unionDF = masterDF.union(deltaDF)

    return unionDF
  }

  def unionDataFiles(sparkSession:SparkSession,singleFile:String,csvDelimiter:String,masterFile:String,deltaFile:String,outputPath:String)= {

    // Load full load file
    printCurrentTime("Start of Loading Master File",28)
    val fullDF = loadCSV(sparkSession,csvDelimiter,masterFile)
    printCurrentTime("End of Loading Master File",28)

    // Load delta file
    printCurrentTime("Start of Loading Delta File",28)
    val deltaDF = loadCSV(sparkSession,csvDelimiter,deltaFile)
    printCurrentTime("End of Loading Delta File",28)

    // Merge Files
    printCurrentTime("Start of UNION Operation",28)
    var unionDF = unionDataFrames(fullDF,deltaDF)
    printCurrentTime("End of UNION Operation",28)

    // Save Merged Files
    printCurrentTime("Start File Save",28)
    saveCSV(unionDF,singleFile,csvDelimiter,outputPath)
    printCurrentTime("End File Save",28)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    printCurrentTime("Start UNION Program",28)
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val singleFile = args(0)toUpperCase()
    val csvDelimiter = args(1)
    val appName = args(2)
    val sparkMaster = args(3)
    val masterFile = args(4)
    val deltaFile = args(5)
    val outputPath = args(6)

    // Use new SparkSession interface in Spark 2.0
    val sparkSession = SparkSession
      .builder
      .appName(appName)
      .master(sparkMaster)
      .config("spark.app.id",appName)
      .getOrCreate()

    unionDataFiles(sparkSession,singleFile,csvDelimiter,masterFile,deltaFile,outputPath)
    printCurrentTime("End UNION Program",28)
  }
}