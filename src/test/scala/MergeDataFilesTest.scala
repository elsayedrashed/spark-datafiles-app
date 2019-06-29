package main.scala

import org.scalatest.FunSuite

class MergeDataFilesTest extends FunSuite {

  test("Test Case 1: Merge Data Files") {

    val singleFile = "S"
    val csvDelimiter = "|"
    val appName = "E2K"
    val sparkMaster = "local[*]"
    val keyColumns = "anime_id"
    val tsColumn = "update_ts"
    val masterFile = "src/test/resources/input/fullload.csv"
    val deltaFile = "src/test/resources/input/delta.csv"
    val outputPath = "src/test/resources/output/merge"

    MergeDataFiles.main(Array(singleFile,csvDelimiter,appName,sparkMaster,masterFile,deltaFile,keyColumns,tsColumn,outputPath))
    assert(1 === 1)
  }

  test("Test Case 2: Merge Data Files") {

    val singleFile = "S"
    val csvDelimiter = ","
    val appName = "E2K"
    val sparkMaster = "local[*]"
    val keyColumns = "anime_id"
    val tsColumn = "update_ts"
    val masterFile = "src/test/resources/input/fullloadquote1.csv"
    val deltaFile = "src/test/resources/input/deltaquote.csv"
    val outputPath = "src/test/resources/output/mergequote"

    MergeDataFiles.main(Array(singleFile,csvDelimiter,appName,sparkMaster,masterFile,deltaFile,keyColumns,tsColumn,outputPath))
    assert(1 === 1)
  }
}
