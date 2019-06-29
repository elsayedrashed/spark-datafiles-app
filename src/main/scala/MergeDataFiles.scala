package main.scala

import main.scala.MergeUtils._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import java.util.Calendar

object MergeDataFiles {

  def mergeDataFrames(masterDF: DataFrame, deltaDF: DataFrame, keyColumns: String, tsColumnName: String): DataFrame = {

    val partCols = keyColumns.split(",").map(masterDF.col(_))

    val unionDF = masterDF.union(deltaDF)

    val mergedDF = unionDF
      .withColumn("SPARK_ROW_NUMBER", row_number() over Window
        .partitionBy(partCols:_*)
        .orderBy(desc(tsColumnName))
      )
      .where(col("SPARK_ROW_NUMBER") === "1")
      .drop("SPARK_ROW_NUMBER")

    return mergedDF
  }

  def mergeDataFiles(sparkSession:SparkSession,singleFile:String,csvDelimiter:String,masterFile:String,deltaFile:String,keyColumns:String,tsColumn:String,outputPath:String)= {

      // Load full load file
    printCurrentTime("Start of Loading Master File",28)
    val fullDF = loadCSV(sparkSession,csvDelimiter,masterFile)
    printCurrentTime("End of Loading Master File",28)

    // Load delta file
    printCurrentTime("Start of Loading Delta File",28)
    val deltaDF = loadCSV(sparkSession,csvDelimiter,deltaFile)
    printCurrentTime("End of Loading Delta File",28)

    // Merge Files
    printCurrentTime("Start of Merge Operation",28)
    var mergedDF = mergeDataFrames(fullDF,deltaDF,keyColumns,tsColumn)
    printCurrentTime("End of Merge Operation",28)

    // Save Merged Files
    printCurrentTime("Start file save",28)
    saveCSV(mergedDF,singleFile,csvDelimiter,outputPath)
    printCurrentTime("End file save",28)
  }

  /** main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val singleFile = args(0)toUpperCase()
    val csvDelimiter = args(1)
    val appName = args(2)
    val sparkMaster = args(3)
    val masterFile = args(4)
    val deltaFile = args(5)
    val keyColumns = args(6)
    val tsColumn = args(7)
    val outputPath = args(8)

    // Use new SparkSession interface in Spark 2.0
    val sparkSession = SparkSession
      .builder
      .appName(appName)
      .master(sparkMaster)
      .getOrCreate()

    println("Application Id".padTo(30,' ')+ " : " + sparkSession.sparkContext.applicationId)
    printCurrentTime("Start Merge " + appName + " Program",28)
    mergeDataFiles(sparkSession,singleFile,csvDelimiter,masterFile,deltaFile,keyColumns,tsColumn,outputPath)
    printCurrentTime("End Merge " + appName + " Program",28)
  }
}